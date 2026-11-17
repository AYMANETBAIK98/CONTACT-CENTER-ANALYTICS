"""GOLD: silver (clean tables) -> gold (business tables ready for dashboards and dbt).

    docker compose run --rm spark spark-submit spark/gold.py

Gold tables (Parquet in s3a://gold/, and copied to PostgreSQL schema "lakehouse"):
  interval_kpis  one row per market and 15-minute interval: volume vs forecast, SL, abandon, ASA, AHT,
                 occupancy, adherence (the same KPIs as the real-time layer, recomputed in batch)
  agent_daily    one row per agent and day: late login, pause overrun, contacts, AHT, adherence, absence
  daily_market   one row per market and day: the numbers of the end-of-day report
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.lake import path, spark_session

INTERVAL_SECONDS = 900
PRODUCTIVE = ["AVAILABLE", "ON_CONTACT", "ACW"]
BUSY = ["ON_CONTACT", "ACW"]
PAUSES = ["BREAK", "LUNCH"]


def read(spark: SparkSession, dataset: str) -> DataFrame:
    return spark.read.parquet(path("silver", dataset))


def floor_15(col) -> F.Column:
    """Start of the 15-minute interval of a timestamp."""
    return F.timestamp_seconds(F.floor(F.unix_timestamp(col) / INTERVAL_SECONDS) * INTERVAL_SECONDS)


def cut_by_interval(df: DataFrame) -> DataFrame:
    """Split each [start_ts, end_ts) row into one row per 15-minute interval it covers,
    with the seconds spent inside that interval (same rule as the real-time layer)."""
    starts = F.sequence(floor_15("start_ts"),
                        floor_15(F.col("end_ts") - F.expr("INTERVAL 1 MICROSECOND")),
                        F.expr("INTERVAL 15 MINUTES"))
    cut = df.withColumn("interval_start", F.explode(starts))
    interval_end = F.col("interval_start") + F.expr("INTERVAL 15 MINUTES")
    return (cut.withColumn("seg_start", F.greatest("start_ts", "interval_start"))
               .withColumn("seg_end", F.least("end_ts", interval_end))
               .withColumn("seg_seconds", F.col("seg_end").cast("double") - F.col("seg_start").cast("double")))


def overlap_seconds(a0, a1, b0, b1) -> F.Column:
    return F.greatest(F.lit(0.0), F.least(a1, b1).cast("double") - F.greatest(a0, b0).cast("double"))


# ---------------------------------------------------------------- interval KPIs
def contact_kpis(contacts: DataFrame) -> DataFrame:
    return (contacts.withColumn("interval_start", floor_15("enqueue_ts"))
            .groupBy("date", "market", "interval_start")
            .agg(F.count("*").alias("offered"),
                 F.sum(F.col("is_answered").cast("int")).alias("answered"),
                 F.sum((~F.col("is_answered")).cast("int")).alias("abandoned"),
                 F.sum(F.col("is_in_service_level").cast("int")).alias("answered_in_sl"),
                 F.avg(F.when(F.col("is_answered"), F.col("wait_seconds"))).alias("asa_seconds"),
                 F.avg(F.col("talk_seconds") + F.col("acw_seconds")).alias("aht_seconds")))


def agent_time(states: DataFrame, schedule: DataFrame) -> DataFrame:
    """Productive, busy and adherent seconds per market and interval.
    Adherent = productive during the shift but outside planned pauses, or on pause during a planned pause."""
    segs = cut_by_interval(states)
    base = (segs.groupBy("date", "market", "interval_start")
            .agg(F.sum(F.when(F.col("state").isin(PRODUCTIVE), F.col("seg_seconds")).otherwise(0)).alias("productive_seconds"),
                 F.sum(F.when(F.col("state").isin(BUSY), F.col("seg_seconds")).otherwise(0)).alias("busy_seconds")))
    plan = schedule.select("date", "agent_username", "activity",
                           F.col("start_ts").alias("p_start"), F.col("end_ts").alias("p_end"))
    joined = segs.join(plan, ["date", "agent_username"]).where(
        (F.col("p_start") < F.col("seg_end")) & (F.col("p_end") > F.col("seg_start")))
    o = overlap_seconds(F.col("seg_start"), F.col("seg_end"), F.col("p_start"), F.col("p_end"))
    productive, pause = F.col("state").isin(PRODUCTIVE), F.col("state").isin(PAUSES)
    signed = (F.when((F.col("activity") == "SHIFT") & productive, o)          # working during the shift...
               .when(F.col("activity").isin(PAUSES) & productive, -o)         # ...but not during a planned pause
               .when(F.col("activity").isin(PAUSES) & pause, o)               # on pause when a pause is planned
               .otherwise(0.0))
    adherent = joined.groupBy("date", "market", "interval_start").agg(F.sum(signed).alias("adherent_seconds"))
    shifts = cut_by_interval(schedule.where(F.col("activity") == "SHIFT"))
    scheduled = shifts.groupBy("date", "market", "interval_start").agg(F.sum("seg_seconds").alias("scheduled_seconds"))
    return (base.join(adherent, ["date", "market", "interval_start"], "full")
                .join(scheduled, ["date", "market", "interval_start"], "full"))


def build_interval_kpis(contacts, states, schedule, forecast) -> DataFrame:
    keys = ["date", "market", "interval_start"]
    df = (forecast.join(contact_kpis(contacts), keys, "full")
                  .join(agent_time(states, schedule), keys, "full")
                  .fillna(0, ["offered", "answered", "abandoned", "answered_in_sl", "productive_seconds",
                              "busy_seconds", "adherent_seconds", "scheduled_seconds"]))
    ratio = lambda a, b: F.when(F.col(b) > 0, F.col(a) / F.col(b))
    return df.select(
        *keys, "forecast_contacts", "forecast_aht_seconds", "offered", "answered", "abandoned", "answered_in_sl",
        ratio("answered_in_sl", "offered").alias("service_level"),
        ratio("abandoned", "offered").alias("abandon_rate"),
        "asa_seconds", "aht_seconds",
        F.when(F.col("forecast_contacts") > 0, F.col("offered") / F.col("forecast_contacts") - 1).alias("forecast_error"),
        ratio("busy_seconds", "productive_seconds").alias("occupancy"),
        ratio("adherent_seconds", "scheduled_seconds").alias("adherence"),
        (F.col("productive_seconds") / INTERVAL_SECONDS).alias("agents_productive"),
        (F.col("scheduled_seconds") / INTERVAL_SECONDS).alias("agents_scheduled"),
    )


# ---------------------------------------------------------------- agent daily
def build_agent_daily(contacts, states, schedule) -> DataFrame:
    keys = ["date", "market", "agent_username"]
    shift = (schedule.where(F.col("activity") == "SHIFT")
             .select(*keys, F.col("start_ts").alias("shift_start"), F.col("end_ts").alias("shift_end")))
    planned_pause = (schedule.where(F.col("activity").isin(PAUSES))
                     .withColumn("s", F.col("end_ts").cast("double") - F.col("start_ts").cast("double"))
                     .groupBy(*keys).agg(F.sum("s").alias("planned_pause_seconds")))
    actual = (states.groupBy(*keys).agg(
        F.min("start_ts").alias("first_login"), F.max("end_ts").alias("last_logout"),
        F.sum(F.when(F.col("state").isin(PRODUCTIVE), F.col("seconds")).otherwise(0)).alias("productive_seconds"),
        F.sum(F.when(F.col("state").isin(PAUSES), F.col("seconds")).otherwise(0)).alias("pause_seconds")))
    handled = (contacts.where("is_answered")
               .groupBy("date", "market", F.col("agent_username"))
               .agg(F.count("*").alias("contacts_handled"),
                    F.avg(F.col("talk_seconds") + F.col("acw_seconds")).alias("aht_seconds")))
    # adherence per agent and day, from the interval cut (same rule as interval_kpis)
    segs = cut_by_interval(states)
    plan = schedule.select("date", "agent_username", "activity",
                           F.col("start_ts").alias("p_start"), F.col("end_ts").alias("p_end"))
    o = overlap_seconds(F.col("seg_start"), F.col("seg_end"), F.col("p_start"), F.col("p_end"))
    productive, pause = F.col("state").isin(PRODUCTIVE), F.col("state").isin(PAUSES)
    signed = (F.when((F.col("activity") == "SHIFT") & productive, o)
               .when(F.col("activity").isin(PAUSES) & productive, -o)
               .when(F.col("activity").isin(PAUSES) & pause, o).otherwise(0.0))
    adherent = (segs.join(plan, ["date", "agent_username"])
                .where((F.col("p_start") < F.col("seg_end")) & (F.col("p_end") > F.col("seg_start")))
                .groupBy(*keys).agg(F.sum(signed).alias("adherent_seconds")))
    df = (shift.join(actual, keys, "left").join(planned_pause, keys, "left")
               .join(handled, keys, "left").join(adherent, keys, "left"))
    scheduled = F.col("shift_end").cast("double") - F.col("shift_start").cast("double")
    return df.select(
        *keys, "shift_start", "shift_end", "first_login", "last_logout",
        F.col("first_login").isNull().alias("is_absent"),
        F.greatest(F.lit(0.0), (F.col("first_login").cast("double") - F.col("shift_start").cast("double")) / 60)
         .alias("late_login_minutes"),
        F.greatest(F.lit(0.0), (F.col("pause_seconds") - F.col("planned_pause_seconds")) / 60).alias("pause_overrun_minutes"),
        F.coalesce("contacts_handled", F.lit(0)).alias("contacts_handled"),
        "aht_seconds", "productive_seconds",
        (F.coalesce("adherent_seconds", F.lit(0.0)) / scheduled).alias("adherence"),
    )


# ---------------------------------------------------------------- daily market
def build_daily_market(interval_kpis: DataFrame, agent_daily: DataFrame) -> DataFrame:
    day = interval_kpis.groupBy("date", "market").agg(
        F.sum("forecast_contacts").alias("forecast_contacts"),
        F.sum("offered").alias("offered"), F.sum("answered").alias("answered"),
        F.sum("abandoned").alias("abandoned"), F.sum("answered_in_sl").alias("answered_in_sl"),
        F.sum(F.col("asa_seconds") * F.col("answered")).alias("_wait"),
        F.sum(F.when(F.col("offered") > 0, (F.col("service_level") < 0.8).cast("int"))).alias("intervals_below_target"),
        F.sum((F.col("offered") > 0).cast("int")).alias("intervals_open"))
    agents = agent_daily.groupBy("date", "market").agg(
        F.count("*").alias("agents_scheduled"), F.sum(F.col("is_absent").cast("int")).alias("agents_absent"),
        F.avg(F.when(~F.col("is_absent"), F.col("adherence"))).alias("adherence_present_agents"))
    return day.join(agents, ["date", "market"], "left").select(
        "date", "market", "forecast_contacts", "offered", "answered", "abandoned",
        (F.col("answered_in_sl") / F.col("offered")).alias("service_level"),
        (F.col("abandoned") / F.col("offered")).alias("abandon_rate"),
        (F.col("_wait") / F.col("answered")).alias("asa_seconds"),
        (F.col("offered") / F.col("forecast_contacts") - 1).alias("forecast_error"),
        "intervals_below_target", "intervals_open", "agents_scheduled", "agents_absent", "adherence_present_agents")


JDBC_URL = (f"jdbc:postgresql://{os.getenv('WAREHOUSE_HOST', 'postgres')}:{os.getenv('WAREHOUSE_PORT', '5432')}/"
            f"{os.getenv('WAREHOUSE_DB', 'contact_center')}")
JDBC_USER, JDBC_PASSWORD = os.getenv("WAREHOUSE_USER", "cc"), os.getenv("WAREHOUSE_PASSWORD", "cc")


def run_sql(spark: SparkSession, sql: str) -> None:
    """Run one SQL statement in PostgreSQL through the JDBC driver Spark already loaded."""
    jvm = spark._jvm
    loader = jvm.Thread.currentThread().getContextClassLoader()
    driver = jvm.java.lang.Class.forName("org.postgresql.Driver", True, loader).newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", JDBC_USER)
    props.setProperty("password", JDBC_PASSWORD)
    conn = driver.connect(JDBC_URL, props)
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()


def export_to_postgres(df: DataFrame, table: str) -> None:
    """Copy a gold table to PostgreSQL (schema lakehouse) for dbt and Power BI. Replaced at each run."""
    (df.write.format("jdbc").mode("overwrite")
       .option("url", JDBC_URL).option("dbtable", f"lakehouse.{table}")
       .option("user", JDBC_USER).option("password", JDBC_PASSWORD)
       .option("driver", "org.postgresql.Driver").option("truncate", "true")
       .save())


def main() -> None:
    spark = spark_session("gold")
    contacts, states = read(spark, "contacts"), read(spark, "agent_states")
    schedule, forecast = read(spark, "schedule"), read(spark, "forecast")
    interval_kpis = build_interval_kpis(contacts, states, schedule, forecast).cache()
    agent_daily = build_agent_daily(contacts, states, schedule).cache()
    tables = {"interval_kpis": interval_kpis, "agent_daily": agent_daily,
              "daily_market": build_daily_market(interval_kpis, agent_daily)}
    to_postgres = os.getenv("EXPORT_TO_POSTGRES", "1") == "1"
    if to_postgres:
        run_sql(spark, "CREATE SCHEMA IF NOT EXISTS lakehouse")
    for name, df in tables.items():
        df.write.mode("overwrite").partitionBy("date").parquet(path("gold", name))
        if to_postgres:
            export_to_postgres(df, name)
        print(f"gold.{name:<14} {df.count():>7,} rows -> {path('gold', name)}"
              + (f" + PostgreSQL lakehouse.{name}" if to_postgres else ""))
    spark.stop()


if __name__ == "__main__":
    main()
