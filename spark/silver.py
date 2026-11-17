"""SILVER: bronze (raw files) -> silver (clean, typed, deduplicated Parquet tables).

    docker compose run --rm spark spark-submit spark/silver.py

For each dataset:
  1. read the raw files with an explicit schema (no guessing)
  2. flatten nested JSON, convert text timestamps to real timestamps
  3. apply data-quality rules: bad rows go to a quarantine table, with the reason, instead of disappearing
  4. remove duplicates (same contact / same event id)
  5. write Parquet, partitioned by date and market

Silver tables:
  contacts      one row per contact, flat
  agent_states  one row per agent state with its start, end and duration (built with a window function)
  schedule      planned shifts, breaks and lunches
  forecast      forecast contacts and handle time per 15-minute interval
  quarantine    rows rejected by a quality rule, with the reason
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark.lake import path, spark_session

TS = "yyyy-MM-dd'T'HH:mm:ss.SSSX"          # 2026-11-04T08:00:06.232Z

CONTACT_SCHEMA = T.StructType([
    T.StructField("ContactId", T.StringType()),
    T.StructField("Channel", T.StringType()),
    T.StructField("InitiationMethod", T.StringType()),
    T.StructField("InitiationTimestamp", T.StringType()),
    T.StructField("Queue", T.StructType([
        T.StructField("Name", T.StringType()),
        T.StructField("EnqueueTimestamp", T.StringType()),
        T.StructField("DequeueTimestamp", T.StringType()),
        T.StructField("Duration", T.LongType()),
    ])),
    T.StructField("Agent", T.StructType([
        T.StructField("Username", T.StringType()),
        T.StructField("ConnectedToAgentTimestamp", T.StringType()),
        T.StructField("AgentInteractionDuration", T.LongType()),
        T.StructField("AfterContactWorkStartTimestamp", T.StringType()),
        T.StructField("AfterContactWorkEndTimestamp", T.StringType()),
        T.StructField("AfterContactWorkDuration", T.LongType()),
    ])),
    T.StructField("Attributes", T.StructType([
        T.StructField("market", T.StringType()),
        T.StructField("language", T.StringType()),
    ])),
    T.StructField("DisconnectTimestamp", T.StringType()),
    T.StructField("DisconnectReason", T.StringType()),
])

AGENT_EVENT_SCHEMA = T.StructType([
    T.StructField("EventId", T.StringType()),
    T.StructField("AgentUsername", T.StringType()),
    T.StructField("Market", T.StringType()),
    T.StructField("EventTimestamp", T.StringType()),
    T.StructField("State", T.StringType()),
])

STATES = ["LOGIN", "AVAILABLE", "ON_CONTACT", "ACW", "BREAK", "LUNCH", "LOGOUT"]


def ts(col: str):
    return F.to_timestamp(F.col(col), TS)


def split_quality(df: DataFrame, rules: dict[str, F.Column], dataset: str) -> tuple[DataFrame, DataFrame]:
    """rules = {reason: condition that is TRUE for a bad row}.
    Returns (good rows, quarantined rows with the first broken rule as reason)."""
    reason = F.lit(None).cast("string")
    for name, broken in reversed(list(rules.items())):
        reason = F.when(F.coalesce(broken, F.lit(False)), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("_reason", reason)
    good = tagged.filter(F.col("_reason").isNull()).drop("_reason")
    bad = tagged.filter(F.col("_reason").isNotNull()).select(
        F.lit(dataset).alias("dataset"), F.col("_reason").alias("reason"),
        F.to_json(F.struct(*[c for c in df.columns])).alias("record"), "date", "market")
    return good, bad


# ---------------------------------------------------------------- contacts
def build_contacts(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    raw = spark.read.schema(CONTACT_SCHEMA).json(path("bronze", "contacts"))   # date, market come from folders
    flat = raw.select(
        F.col("ContactId").alias("contact_id"),
        "date", "market",
        F.col("Queue.Name").alias("queue"),
        F.col("Attributes.language").alias("language"),
        ts("Queue.EnqueueTimestamp").alias("enqueue_ts"),
        ts("Queue.DequeueTimestamp").alias("dequeue_ts"),
        F.col("Queue.Duration").alias("wait_seconds"),
        F.col("Agent").isNotNull().alias("is_answered"),
        F.col("Agent.Username").alias("agent_username"),
        ts("Agent.ConnectedToAgentTimestamp").alias("connected_ts"),
        F.col("Agent.AgentInteractionDuration").alias("talk_seconds"),
        F.col("Agent.AfterContactWorkDuration").alias("acw_seconds"),
        ts("Agent.AfterContactWorkEndTimestamp").alias("acw_end_ts"),
        ts("DisconnectTimestamp").alias("disconnect_ts"),
        F.col("DisconnectReason").alias("disconnect_reason"),
    )
    good, bad = split_quality(flat, {
        "missing contact id": F.col("contact_id").isNull(),
        "unreadable enqueue timestamp": F.col("enqueue_ts").isNull(),
        "dequeue before enqueue": F.col("dequeue_ts") < F.col("enqueue_ts"),
        "negative wait": F.col("wait_seconds") < 0,
        "answered without agent": F.col("is_answered") & F.col("agent_username").isNull(),
        "negative talk or wrap-up": F.col("is_answered") & ((F.col("talk_seconds") < 0) | (F.col("acw_seconds") < 0)),
    }, "contacts")
    # the same contact delivered twice (e.g. a file exported twice) is kept once
    good = good.dropDuplicates(["contact_id"]).withColumn(
        "is_in_service_level", F.col("is_answered") & (F.col("wait_seconds") <= 20))
    return good, bad


# ---------------------------------------------------------------- agent states
def build_agent_states(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    raw = spark.read.schema(AGENT_EVENT_SCHEMA).json(path("bronze", "agent_events"))
    events = raw.select(
        F.col("EventId").alias("event_id"), "date", "market",
        F.col("AgentUsername").alias("agent_username"),
        ts("EventTimestamp").alias("event_ts"),
        F.col("State").alias("state"),
    )
    good, bad = split_quality(events, {
        "missing event id": F.col("event_id").isNull(),
        "missing agent": F.col("agent_username").isNull(),
        "unreadable timestamp": F.col("event_ts").isNull(),
        "unknown state": ~F.col("state").isin(STATES),
    }, "agent_events")
    good = good.dropDuplicates(["event_id"])
    # each state lasts until the agent's next event: LEAD() looks at the next row of the same agent and day.
    # Two events can share the same millisecond (LOGIN then AVAILABLE): LOGIN must come first and LOGOUT last,
    # otherwise the agent would stay "LOGIN" (not productive) until the next event.
    order = F.when(F.col("state") == "LOGIN", 0).when(F.col("state") == "LOGOUT", 2).otherwise(1)
    good = good.withColumn("_order", order)
    nxt = Window.partitionBy("date", "agent_username").orderBy("event_ts", "_order")
    states = (good.withColumn("end_ts", F.lead("event_ts").over(nxt))
              .filter(F.col("state") != "LOGOUT")                      # LOGOUT ends the day, it has no duration
              .filter(F.col("end_ts").isNotNull())
              .select("date", "market", "agent_username", "state",
                      F.col("event_ts").alias("start_ts"), "end_ts",
                      (F.col("end_ts").cast("double") - F.col("event_ts").cast("double")).alias("seconds")))
    return states, bad


# ---------------------------------------------------------------- schedule + forecast
def build_schedule(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("header", True).csv(path("bronze", "schedule"))
    return raw.select("date", "market", "agent_username", "activity",
                      ts("start_utc").alias("start_ts"), ts("end_utc").alias("end_ts"))


def build_forecast(spark: SparkSession) -> DataFrame:
    raw = spark.read.option("header", True).csv(path("bronze", "forecast"))
    return raw.select("date", "market", ts("interval_start_utc").alias("interval_start"),
                      F.col("forecast_contacts").cast("double"),
                      F.col("forecast_aht_seconds").cast("int").alias("forecast_aht_seconds"))


def write(df: DataFrame, dataset: str) -> int:
    df.write.mode("overwrite").partitionBy("date", "market").parquet(path("silver", dataset))
    return df.count()


def main() -> None:
    spark = spark_session("silver")
    contacts, bad_contacts = build_contacts(spark)
    states, bad_events = build_agent_states(spark)
    counts = {
        "contacts": write(contacts, "contacts"),
        "agent_states": write(states, "agent_states"),
        "schedule": write(build_schedule(spark), "schedule"),
        "forecast": write(build_forecast(spark), "forecast"),
        "quarantine": write(bad_contacts.unionByName(bad_events), "quarantine"),
    }
    for dataset, n in counts.items():
        print(f"silver.{dataset:<13} {n:>8,} rows -> {path('silver', dataset)}")
    spark.stop()


if __name__ == "__main__":
    main()
