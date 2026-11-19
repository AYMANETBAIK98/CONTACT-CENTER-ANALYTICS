"""LIVE DASHBOARD (step 7a): the real-time analyst's screen, in the browser.

    docker compose up -d dashboard          then open http://localhost:8501

Two modes:
  - Live:   reads realtime.interval_kpis, which the Kafka consumer (step 2) fills while the producer replays a day.
            The page refreshes itself; "now" is the interval the consumer is filling.
  - Replay: no Kafka needed, the day is recomputed from the files and a slider moves the clock.

Everything shown comes from the step 5 / 6 logic (rta/): projection, probability, adherence wallboard, status message.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st

from rta.data import LANDING, load_forecast, load_planned_agents, read_realtime_kpis, replay_kpis
from rta.projection import SL_TARGET, project
from rta.status_message import FLAGS, build_message, probability_on_target
from rta.wallboard import PRIORITY, statuses

st.set_page_config(page_title="Contact Center · Live", page_icon="📞", layout="wide")

BAND_COLOURS = {"Green": "#e5f4e4", "Amber": "#f9eeda", "Red": "#fbebeb", "Absent": "#f0efec"}
STATUS_ICON = {"ON TRACK": "🟢", "SECURED": "🟢", "AT RISK": "🟡", "TARGET LOST": "🔴", "CLOSED": "⚪"}


# ---------------------------------------------------------------- data (cached)
@st.cache_data(show_spinner=False)
def days() -> list[str]:
    return sorted(p.name.split("=")[1] for p in (LANDING / "contacts").glob("date=*"))


@st.cache_data(show_spinner="Reading forecast and schedule…")
def plan(day: str):
    return load_forecast(day), load_planned_agents(day)


@st.cache_data(show_spinner="Replaying the day from the files…")
def replayed(day: str):
    return replay_kpis(day)


@st.cache_data(ttl=10, show_spinner=False)
def live(day: str):
    return read_realtime_kpis(day)


@st.cache_data(ttl=60, show_spinner=False)
def wallboard(day: str, now: datetime):
    return statuses(day, now)


@st.cache_data(ttl=60, show_spinner=False)
def message(day: str, at: str, source: str) -> str:
    return build_message(day, at, source)


# ---------------------------------------------------------------- sidebar
st.sidebar.title("📞 Contact Center")
mode = st.sidebar.radio("Data", ["Live (Kafka → PostgreSQL)", "Replay (files)"], index=1,
                        help="Live needs the producer and the consumer of step 2 running.")
is_live = mode.startswith("Live")
all_days = days()
if not all_days:
    st.error("No data yet: run the generator of step 1 first.")
    st.stop()
day = st.sidebar.selectbox("Day", all_days, index=min(2, len(all_days) - 1))
if not is_live:
    t = st.sidebar.slider("Clock (UTC)", time(7, 0), time(21, 0), time(12, 0), step=timedelta(minutes=15), format="HH:mm")
    refresh = None
else:
    refresh = st.sidebar.select_slider("Refresh every", [5, 10, 30, 60], value=10, format_func=lambda s: f"{s} s")
market_pick = st.sidebar.multiselect("Markets", ["UK", "DE", "ES"], default=["UK", "DE", "ES"])
st.sidebar.caption("Simulated data. Times in UTC: UK = UTC, DE and ES = UTC+1 in November.")


@st.fragment(run_every=refresh)
def page() -> None:
    forecast, planned = plan(day)
    if is_live:
        kpis = live(day)
        starts = [r["interval_start"] for rows in kpis.values() for r in rows]
        if not starts:
            st.info(f"Nothing in realtime.interval_kpis for {day} yet. Start the consumer, then the producer "
                    f"(`python -m streaming.producer --date {day} --speed 120`).")
            return
        now = max(starts)                       # the interval the consumer is filling: earlier ones are closed
    else:
        kpis = replayed(day)
        now = datetime.combine(datetime.fromisoformat(day).date(), t, tzinfo=timezone.utc)
    at = f"{now:%H:%M}"
    st.title(f"Real-time status · {day} · {at} UTC")
    st.caption("Closed intervals only. Projection, probability and adherence from rta/ (steps 5 and 6).")

    markets = [m for m in ("UK", "DE", "ES") if m in forecast and m in market_pick]
    if not markets:
        st.warning("No market open that day.")
        return
    projections = {m: project(m, now, kpis.get(m, []), forecast[m], planned.get(m, {})) for m in markets}

    # ------------------------------------------------ 1. one card per market
    for col, m in zip(st.columns(len(markets)), markets):
        p = projections[m]
        prob = probability_on_target(p)
        with col.container(border=True):
            st.subheader(f"{FLAGS[m]} {m}  {STATUS_ICON[p.status]} {p.status}")
            a, b, c = st.columns(3)
            a.metric("SL so far", "-" if p.sl_so_far is None else f"{100 * p.sl_so_far:.0f}%",
                     None if p.sl_so_far is None else f"{100 * (p.sl_so_far - SL_TARGET):+.0f} pts")
            b.metric("EOD projection", "-" if p.projected_sl is None else f"{100 * p.projected_sl:.0f}%")
            c.metric("P(close ≥ 80%)", f"{100 * prob:.0f}%")
            st.caption(f"{p.offered_so_far} offered · volume {100 * (p.volume_factor - 1):+.0f}% vs forecast · "
                       f"{'-' if p.required_sl is None else f'{100 * p.required_sl:.0f}%'} SL needed from now")

    # ------------------------------------------------ 2. intraday charts
    rows = []
    for m in markets:
        for r in kpis.get(m, []):
            if r["interval_start"] < now and r["offered"]:
                rows.append({"market": m, "interval": r["interval_start"], "SL": r["answered_in_sl"] / r["offered"],
                             "offered": r["offered"]})
        for s, (calls, _) in forecast[m].items():
            rows.append({"market": m, "interval": s, "forecast": calls})
    df = pd.DataFrame(rows)
    left, right = st.columns(2)
    with left:
        st.markdown("**Service level per interval** (target 80/20)")
        sl = df.dropna(subset=["SL"]) if "SL" in df else pd.DataFrame()
        if not sl.empty:
            base = alt.Chart(sl).encode(x=alt.X("interval:T", title=None, axis=alt.Axis(format="%H:%M")),
                                        y=alt.Y("SL:Q", title=None, axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                                        color=alt.Color("market:N", title=None))
            target = alt.Chart(pd.DataFrame({"y": [SL_TARGET]})).mark_rule(strokeDash=[4, 4], color="gray").encode(y="y:Q")
            st.altair_chart(base.mark_line(point=True) + target, width="stretch")
    with right:
        st.markdown("**Offered vs forecast** (calls per 15 min)")
        vol = df.melt(id_vars=["market", "interval"], value_vars=[c for c in ("offered", "forecast") if c in df],
                      var_name="series", value_name="calls").dropna()
        chart = alt.Chart(vol).mark_line().encode(
            x=alt.X("interval:T", title=None, axis=alt.Axis(format="%H:%M")), y=alt.Y("calls:Q", title=None),
            color=alt.Color("market:N", title=None), strokeDash=alt.StrokeDash("series:N", title=None))
        st.altair_chart(chart, width="stretch")

    # ------------------------------------------------ 3. staffing gaps and adherence exceptions
    left, right = st.columns(2)
    with left:
        st.markdown("**Rest of day: intervals short of agents** (Erlang C, calibrated)")
        gaps = [{"market": m, "interval": f"{i.interval_start:%H:%M}", "calls": i.calls,
                 "agents expected": i.agents_expected, "agents needed": i.agents_needed, "gap": i.agents_gap,
                 "expected SL": i.expected_sl}
                for m in markets for i in projections[m].intervals if i.agents_gap <= -1]
        if gaps:
            st.dataframe(pd.DataFrame(gaps).sort_values("gap").head(12), hide_index=True, width="stretch",
                         column_config={"expected SL": st.column_config.ProgressColumn(format="percent", min_value=0, max_value=1),
                                        "gap": st.column_config.NumberColumn(format="%+.1f"),
                                        "agents expected": st.column_config.NumberColumn(format="%.1f")})
        else:
            st.success("No interval short of agents for the rest of the day.")
    with right:
        st.markdown("**Adherence exceptions, most urgent first**")
        agents = [a for a in wallboard(day, now) if a.market in markets]
        exc = sorted((a for a in agents if a.exception and (a.minutes_out >= 1 or a.exception == "NOT LOGGED IN")), key=lambda a: (PRIORITY.get(a.exception, 9), -a.minutes_out))
        if exc:
            table = pd.DataFrame([{"agent": a.agent, "exception": a.exception, "minutes": a.minutes_out,
                                   "planned": a.planned_now, "doing": a.actual_now or "-",
                                   "adherence": None if a.adherence is None else 100 * a.adherence, "band": a.band} for a in exc])
            st.dataframe(table.style.apply(lambda r: [f"background-color: {BAND_COLOURS.get(r['band'], '')}"] * len(r), axis=1),
                         hide_index=True, width="stretch",
                         column_config={"adherence": st.column_config.NumberColumn(format="%.0f%%"),
                                        "minutes": st.column_config.NumberColumn(format="%.0f")})
        else:
            st.success("Everyone in adherence.")
        bands = pd.Series([a.band for a in agents if a.band != "Not started"]).value_counts()
        st.caption(" · ".join(f"{b} {bands.get(b, 0)}" for b in ("Green", "Amber", "Red", "Absent")))

    # ------------------------------------------------ 4. the status message, ready to paste
    with st.expander("📨 Intraday status message (step 6), ready to paste", expanded=False):
        st.code(message(day, at, "db" if is_live else "files"), language=None)


page()
