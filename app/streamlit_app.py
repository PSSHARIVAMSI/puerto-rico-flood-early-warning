from __future__ import annotations

import os
import signal
import sys
import threading
import time
from pathlib import Path

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
EARLY_REPO_ROOT = APP_DIR.parent
if str(EARLY_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(EARLY_REPO_ROOT))

from scripts.factors.social_adjustments import (
    build_adjustment_factor_reference,
    load_social_adjustment_spec,
)


def find_repo_root(start: Path | None = None) -> Path:
    probe = (start or Path.cwd()).resolve()
    for candidate in [probe, *probe.parents]:
        if (candidate / "README.md").exists() and (candidate / "JupyterNotebooks").exists():
            return candidate
    return probe


def load_view(con: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    try:
        return con.execute(query).df()
    except duckdb.Error:
        return pd.DataFrame()


PRIORITY_BAND_STYLES: dict[str, dict[str, object]] = {
    "Red": {"color": [204, 39, 64], "description": "Highest adjusted priority index"},
    "Orange": {"color": [241, 124, 45], "description": "Elevated adjusted priority index"},
    "Yellow": {"color": [242, 201, 76], "description": "Watch adjusted priority index"},
    "Green": {"color": [74, 173, 109], "description": "Lower adjusted priority index"},
    "Unbanded": {"color": [148, 163, 184], "description": "Adjusted priority index without a band"},
}

TELEMETRY_SOURCE_STYLES: dict[str, dict[str, object]] = {
    "Flood hazard feed": {
        "color": [31, 119, 180],
        "description": "Station comes from the indexed flood-hazard telemetry feed.",
    },
    "NOAA water-level feed": {
        "color": [23, 190, 207],
        "description": "Station comes only from the NOAA water-level summary feed.",
    },
    "Unknown telemetry source": {
        "color": [107, 114, 128],
        "description": "Station source path could not be classified from the local baseline.",
    },
}


def style_for_priority_band(band: object) -> dict[str, object]:
    return PRIORITY_BAND_STYLES.get(str(band), PRIORITY_BAND_STYLES["Unbanded"])


def style_for_telemetry_source(source_type: object) -> dict[str, object]:
    return TELEMETRY_SOURCE_STYLES.get(str(source_type), TELEMETRY_SOURCE_STYLES["Unknown telemetry source"])


def scaled_radius(
    value: object,
    *,
    min_radius: int,
    max_radius: int,
    fallback: int,
    domain_min: float = 0.0,
    domain_max: float = 100.0,
) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return fallback
    if pd.isna(numeric):
        return fallback
    if domain_max <= domain_min:
        return fallback
    clipped = min(max(numeric, domain_min), domain_max)
    share = (clipped - domain_min) / (domain_max - domain_min)
    return int(min_radius + share * (max_radius - min_radius))


def format_metric(value: object, digits: int = 1, suffix: str = "") -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric:.{digits}f}{suffix}"


def prepare_municipio_map_points(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"municipio", "latitude", "longitude"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()

    map_df = frame.copy()
    numeric_cols = [
        "latitude",
        "longitude",
        "priority_index_conf_adj",
        "hazard_combined",
        "vulnerability_score",
        "response_readiness_index",
    ]
    for column in numeric_cols:
        if column in map_df.columns:
            map_df[column] = pd.to_numeric(map_df[column], errors="coerce")
    map_df = map_df.dropna(subset=["latitude", "longitude"]).copy()
    if map_df.empty:
        return map_df

    priority_band_series = (
        map_df["priority_band"] if "priority_band" in map_df.columns else pd.Series("Unbanded", index=map_df.index)
    )
    phase_series = map_df["phase"] if "phase" in map_df.columns else pd.Series("", index=map_df.index)
    map_df["priority_band"] = priority_band_series.fillna("Unbanded")
    map_df["phase"] = phase_series.fillna("")
    map_df["color"] = map_df["priority_band"].map(lambda value: style_for_priority_band(value)["color"])
    map_df["radius"] = map_df.get("priority_index_conf_adj", pd.Series(index=map_df.index)).map(
        lambda value: scaled_radius(value, min_radius=12000, max_radius=32000, fallback=15000)
    )
    map_df["point_family"] = "Municipio index"
    map_df["title"] = map_df["municipio"].fillna("Unknown municipio")
    map_df["subtitle"] = "Adjusted municipio priority index"
    map_df["main_index"] = map_df.get("priority_index_conf_adj", pd.Series(index=map_df.index)).map(
        lambda value: f"Adjusted priority index: {format_metric(value)}"
    )
    map_df["secondary_index"] = (
        "Band: "
        + map_df["priority_band"].astype(str)
        + " | Phase: "
        + map_df["phase"].replace("", "n/a").astype(str)
    )
    map_df["detail_1"] = map_df.get("hazard_combined", pd.Series(index=map_df.index)).map(
        lambda value: f"Hazard combined: {format_metric(value)}"
    )
    map_df["detail_2"] = map_df.get("vulnerability_score", pd.Series(index=map_df.index)).map(
        lambda value: f"Vulnerability score: {format_metric(value)}"
    )
    map_df["detail_3"] = map_df.get("response_readiness_index", pd.Series(index=map_df.index)).map(
        lambda value: f"Response readiness: {format_metric(value)}"
    )
    return map_df


def telemetry_radius(telemetry_index: object, source_count: object) -> int:
    base = scaled_radius(telemetry_index, min_radius=8000, max_radius=18000, fallback=10000)
    try:
        source_count_value = int(source_count)
    except (TypeError, ValueError):
        source_count_value = 0
    return base + 1500 if source_count_value >= 2 else base


def prepare_telemetry_map_points(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"station_id", "station_name", "latitude", "longitude"}
    if frame.empty or not required.issubset(frame.columns):
        return pd.DataFrame()

    station_df = frame.copy()
    numeric_cols = [
        "latitude",
        "longitude",
        "latest_water_level",
        "rise_rate_per_hour",
        "sensor_hazard_score",
        "flood_hazard_final",
        "telemetry_index",
        "noaa_latest_value",
        "obs_count",
        "telemetry_source_count",
    ]
    for column in numeric_cols:
        if column in station_df.columns:
            station_df[column] = pd.to_numeric(station_df[column], errors="coerce")
    station_df = station_df.dropna(subset=["latitude", "longitude"]).copy()
    if station_df.empty:
        return station_df

    rows: list[dict[str, object]] = []
    for _, row in station_df.iterrows():
        has_flood_feed = any(
            pd.notna(row.get(column))
            for column in ["latest_water_level", "rise_rate_per_hour", "sensor_hazard_score", "flood_hazard_final"]
        )
        has_noaa_feed = any(
            pd.notna(row.get(column))
            for column in ["noaa_latest_value", "obs_count", "latest_quality", "peak_value", "mean_value"]
        )
        feed_count = int(has_flood_feed) + int(has_noaa_feed)
        flood_offset = {"latitude": 0.004 if feed_count > 1 else 0.0, "longitude": -0.006 if feed_count > 1 else 0.0}
        noaa_offset = {"latitude": -0.004 if feed_count > 1 else 0.0, "longitude": 0.006 if feed_count > 1 else 0.0}

        if has_flood_feed:
            rows.append(
                {
                    "station_id": str(row.get("station_id")),
                    "station_name": row.get("station_name"),
                    "latitude": float(row.get("latitude")) + flood_offset["latitude"],
                    "longitude": float(row.get("longitude")) + flood_offset["longitude"],
                    "telemetry_api_type": "Flood hazard feed",
                    "color": style_for_telemetry_source("Flood hazard feed")["color"],
                    "radius": telemetry_radius(row.get("flood_hazard_final"), feed_count),
                    "point_family": "Telemetry station",
                    "title": str(row.get("station_name") or row.get("station_id")),
                    "subtitle": "Flood hazard feed",
                    "main_index": f"Flood hazard index: {format_metric(row.get('flood_hazard_final'))}",
                    "secondary_index": (
                        "Sensor hazard: "
                        + format_metric(row.get("sensor_hazard_score"))
                        + " | Rise rate/hr: "
                        + format_metric(row.get("rise_rate_per_hour"), digits=3)
                    ),
                    "detail_1": f"Latest water level: {format_metric(row.get('latest_water_level'), digits=3)}",
                    "detail_2": f"NWS alert override: {format_metric(row.get('nws_global_alert_score'))}",
                    "detail_3": f"Station id: {row.get('station_id')}",
                }
            )
        if has_noaa_feed:
            rows.append(
                {
                    "station_id": str(row.get("station_id")),
                    "station_name": row.get("station_name"),
                    "latitude": float(row.get("latitude")) + noaa_offset["latitude"],
                    "longitude": float(row.get("longitude")) + noaa_offset["longitude"],
                    "telemetry_api_type": "NOAA water-level feed",
                    "color": style_for_telemetry_source("NOAA water-level feed")["color"],
                    "radius": scaled_radius(
                        row.get("obs_count"),
                        min_radius=7000,
                        max_radius=15000,
                        fallback=9000,
                        domain_min=0.0,
                        domain_max=2000.0,
                    ),
                    "point_family": "Telemetry station",
                    "title": str(row.get("station_name") or row.get("station_id")),
                    "subtitle": "NOAA water-level feed",
                    "main_index": f"Latest water level: {format_metric(row.get('noaa_latest_value'), digits=3)}",
                    "secondary_index": (
                        "Observation count: "
                        + format_metric(row.get("obs_count"), digits=0)
                        + " | Peak value: "
                        + format_metric(row.get("peak_value"), digits=3)
                    ),
                    "detail_1": f"Mean value: {format_metric(row.get('mean_value'), digits=3)}",
                    "detail_2": f"Latest quality: {row.get('latest_quality') or 'n/a'}",
                    "detail_3": f"Station id: {row.get('station_id')}",
                }
            )
    return pd.DataFrame(rows)


def color_chip(label: str, color: list[int], description: str) -> str:
    hex_color = "#{:02x}{:02x}{:02x}".format(*color)
    return (
        "<div style='display:flex;align-items:flex-start;gap:0.6rem;margin:0 0 0.45rem 0;'>"
        f"<span style='display:inline-block;width:0.95rem;height:0.95rem;border-radius:999px;background:{hex_color};"
        "border:1px solid rgba(15,23,42,0.25);margin-top:0.15rem;'></span>"
        f"<div><strong>{label}</strong><br/><span style='color:#475569;'>{description}</span></div>"
        "</div>"
    )


def render_map_legend() -> None:
    st.markdown("**Map Legend**")
    legend_left, legend_right = st.columns(2)
    with legend_left:
        st.markdown("**Municipio index dots**")
        municipio_rows = "".join(
            color_chip(label, style["color"], style["description"])
            for label, style in PRIORITY_BAND_STYLES.items()
        )
        st.markdown(municipio_rows, unsafe_allow_html=True)
    with legend_right:
        st.markdown("**Telemetry dots by source/API type**")
        telemetry_rows = "".join(
            color_chip(label, style["color"], style["description"])
            for label, style in TELEMETRY_SOURCE_STYLES.items()
        )
        st.markdown(telemetry_rows, unsafe_allow_html=True)
    st.caption(
        "Municipio dot size scales with the adjusted priority index. Telemetry dot size scales with the "
        "available station telemetry measure. When a station is present in both local telemetry paths, the map "
        "shows two nearby dots so the API types stay visually distinct."
    )


def build_workbench_map(
    municipio_points: pd.DataFrame,
    telemetry_points: pd.DataFrame,
) -> pdk.Deck | None:
    frames = [frame[["latitude", "longitude"]] for frame in [municipio_points, telemetry_points] if not frame.empty]
    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    view_state = pdk.ViewState(
        latitude=float(combined["latitude"].mean()),
        longitude=float(combined["longitude"].mean()),
        zoom=7,
        pitch=0,
    )

    layers: list[pdk.Layer] = []
    if not municipio_points.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=municipio_points,
                get_position="[longitude, latitude]",
                get_fill_color="color",
                get_line_color=[255, 255, 255],
                get_radius="radius",
                filled=True,
                stroked=True,
                pickable=True,
                opacity=0.36,
                line_width_min_pixels=1,
                radius_min_pixels=7,
                radius_max_pixels=28,
            )
        )
    if not telemetry_points.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=telemetry_points,
                get_position="[longitude, latitude]",
                get_fill_color="color",
                get_line_color=[17, 24, 39],
                get_radius="radius",
                filled=True,
                stroked=True,
                pickable=True,
                opacity=0.92,
                line_width_min_pixels=2,
                radius_min_pixels=7,
                radius_max_pixels=22,
            )
        )

    tooltip = {
        "html": (
            "<b>{point_family}</b><br/>"
            "<b>{title}</b><br/>"
            "{subtitle}<br/>"
            "{main_index}<br/>"
            "{secondary_index}<br/>"
            "{detail_1}<br/>"
            "{detail_2}<br/>"
            "{detail_3}"
        ),
        "style": {
            "backgroundColor": "rgba(15, 23, 42, 0.92)",
            "color": "white",
            "fontSize": "12px",
        },
    }
    return pdk.Deck(map_style=pdk.map_styles.LIGHT, initial_view_state=view_state, layers=layers, tooltip=tooltip)


def request_local_shutdown(delay_seconds: float = 1.5) -> None:
    if st.session_state.get("_shutdown_requested"):
        return
    st.session_state["_shutdown_requested"] = True

    def _shutdown() -> None:
        time.sleep(delay_seconds)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=_shutdown, daemon=True).start()


def render_shutdown_notice() -> None:
    st.success(
        "Local app closed successfully. Any active DuckDB connection was released cleanly and the server is shutting down."
    )
    st.info("This page will stop responding in a moment. You can close this browser tab.")
    st.markdown(
        """
        <script>
        window.setTimeout(function() {
          try { window.close(); } catch (e) {}
        }, 1800);
        </script>
        """,
        unsafe_allow_html=True,
    )


REPO_ROOT = find_repo_root()
DEFAULT_DB_PATH = Path(
    os.environ.get(
        "SPRING2026DAEN_DUCKDB_PATH",
        str(REPO_ROOT / "data" / "local" / "duckdb" / "spring2026daen_baseline.duckdb"),
    )
).expanduser()
ADJUSTMENT_SPEC = load_social_adjustment_spec(REPO_ROOT)
ADJUSTMENT_REFERENCE_DF = build_adjustment_factor_reference(ADJUSTMENT_SPEC)

st.set_page_config(page_title="PR Hazard and Readiness Analysis Workbench", layout="wide")

st.title("PR Hazard and Readiness Analysis Workbench")
st.caption(
    "Local/internal workbench only. This app complements the notebook-first workflow and does not replace the "
    "current GitHub Pages public dashboard."
)

db_path_input = st.sidebar.text_input("DuckDB path", str(DEFAULT_DB_PATH))
db_path = Path(db_path_input).expanduser()

st.sidebar.markdown("### Filters")
top_n = st.sidebar.slider("Top municipios in chart", min_value=5, max_value=25, value=10, step=1)
st.sidebar.markdown("### Session")
quit_requested = st.sidebar.button(
    "Quit Local App",
    use_container_width=True,
    help="Close the local DuckDB connection cleanly and stop this local Streamlit server.",
)

if not db_path.exists():
    if quit_requested:
        render_shutdown_notice()
        request_local_shutdown()
        st.stop()
    st.warning(
        "DuckDB baseline not found yet. Build it first with `python scripts/build_duckdb_baseline.py`, "
        "then reload this app."
    )
    st.stop()

con: duckdb.DuckDBPyConnection | None = None
try:
    con = duckdb.connect(str(db_path), read_only=True)

    if quit_requested:
        con.close()
        con = None
        render_shutdown_notice()
        request_local_shutdown()
        st.stop()

    municipio_df = load_view(con, "SELECT * FROM vw_municipio_risk_summary")
    alerts_df = load_view(con, "SELECT * FROM vw_alerts_summary")
    stations_df = load_view(con, "SELECT * FROM vw_station_water_summary")
    source_status_df = load_view(con, "SELECT * FROM vw_baseline_source_status")
    factor_summary_df = load_view(con, "SELECT * FROM vw_vulnerability_factor_summary")
    terrain_summary_df = load_view(con, "SELECT * FROM vw_terrain_summary")
    adjustments_df = load_view(con, "SELECT * FROM vw_vulnerability_adjustments")

    phase_options = sorted(
        [value for value in municipio_df.get("phase", pd.Series(dtype="string")).dropna().unique().tolist()]
    )
    selected_phases = st.sidebar.multiselect("Phase", options=phase_options, default=phase_options)

    band_options = sorted(
        [value for value in municipio_df.get("priority_band", pd.Series(dtype="string")).dropna().unique().tolist()]
    )
    selected_bands = st.sidebar.multiselect("Priority band", options=band_options, default=band_options)

    municipio_search = st.sidebar.text_input("Municipio contains", "")

    filtered_municipios = municipio_df.copy()
    if selected_phases:
        filtered_municipios = filtered_municipios[filtered_municipios["phase"].isin(selected_phases)]
    if selected_bands:
        filtered_municipios = filtered_municipios[filtered_municipios["priority_band"].isin(selected_bands)]
    if municipio_search.strip():
        filtered_municipios = filtered_municipios[
            filtered_municipios["municipio"].fillna("").str.contains(municipio_search.strip(), case=False)
        ]

    metric_cols = st.columns(5)
    metric_cols[0].metric("Filtered municipios", f"{len(filtered_municipios):,}")
    metric_cols[1].metric(
        "Mean adj. priority",
        f"{filtered_municipios['priority_index_conf_adj'].mean():.1f}" if not filtered_municipios.empty else "n/a",
    )
    metric_cols[2].metric("Alerts loaded", f"{len(alerts_df):,}")
    metric_cols[3].metric("Stations loaded", f"{len(stations_df):,}")
    metric_cols[4].metric(
        "Mean age adj. points",
        f"{adjustments_df['age_adjustment_points'].mean():.1f}" if not adjustments_df.empty else "n/a",
    )

    top_municipio = (
        filtered_municipios.sort_values("priority_index_conf_adj", ascending=False).head(1)["municipio"].iloc[0]
        if not filtered_municipios.empty
        else "n/a"
    )
    st.markdown(f"**Current top municipio in this filtered view:** `{top_municipio}`")

    left_col, right_col = st.columns([1.2, 1])

    with left_col:
        st.subheader("Municipio Summary Table")
        table_columns = [
            "municipio",
            "phase",
            "priority_band",
            "priority_index_conf_adj",
            "hazard_combined",
            "vulnerability_score",
            "poverty_score",
            "transport_constraint_score",
            "housing_fragility_score",
            "income_capacity_score",
            "response_readiness_index",
            "terrain_data_completeness",
            "recommended_actions",
        ]
        available_columns = [column for column in table_columns if column in filtered_municipios.columns]
        st.dataframe(
            filtered_municipios[available_columns].sort_values("priority_index_conf_adj", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    with right_col:
        st.subheader("Top Municipios by Adjusted Priority")
        if filtered_municipios.empty:
            st.info("No municipios match the current filters.")
        else:
            chart_df = (
                filtered_municipios.sort_values("priority_index_conf_adj", ascending=False)
                .head(top_n)[["municipio", "priority_index_conf_adj"]]
                .set_index("municipio")
            )
            st.bar_chart(chart_df)

    municipio_map_points = prepare_municipio_map_points(filtered_municipios)
    telemetry_map_points = prepare_telemetry_map_points(stations_df)
    workbench_map = build_workbench_map(municipio_map_points, telemetry_map_points)
    if workbench_map is not None:
        st.subheader("Telemetry and Municipio Index Map")
        st.caption(
            "Filled municipio dots show adjusted priority index by band. Telemetry dots show station-level "
            "feeds colored by source/API type, with hover details for the key indexes now driving the workbench. "
            "Stations present in both telemetry paths appear as two nearby dots."
        )
        st.pydeck_chart(workbench_map, use_container_width=True)
        render_map_legend()
    else:
        st.info("No map-ready municipio or telemetry coordinates are available in the current local baseline.")

    station_col, alert_col = st.columns(2)

    with station_col:
        st.subheader("Station Snapshot")
        if stations_df.empty:
            st.info("No station summary view is available in the current baseline build.")
        else:
            station_cols = [
                "station_id",
                "station_name",
                "telemetry_source_type",
                "telemetry_index",
                "latest_water_level",
                "noaa_latest_value",
                "rise_rate_per_hour",
                "flood_hazard_final",
                "obs_count",
            ]
            station_cols = [column for column in station_cols if column in stations_df.columns]
            station_sort_cols = [
                column for column in ["telemetry_index", "flood_hazard_final", "obs_count"] if column in stations_df.columns
            ]
            station_display_df = stations_df[station_cols]
            if station_sort_cols:
                station_display_df = station_display_df.sort_values(
                    by=station_sort_cols,
                    ascending=False,
                    na_position="last",
                )
            st.dataframe(
                station_display_df,
                use_container_width=True,
                hide_index=True,
            )

    with alert_col:
        st.subheader("Alert Snapshot")
        if alerts_df.empty:
            st.info("No alert summary view is available in the current baseline build.")
        else:
            alert_cols = ["event", "severity", "alert_score", "sent", "ends", "area_desc"]
            alert_cols = [column for column in alert_cols if column in alerts_df.columns]
            st.dataframe(alerts_df[alert_cols], use_container_width=True, hide_index=True)

    with st.expander("Baseline Source Status"):
        if source_status_df.empty:
            st.info("Source status view is unavailable.")
        else:
            st.dataframe(source_status_df, use_container_width=True, hide_index=True)

    with st.expander("Adjustment Factors and Age Overlay"):
        st.caption("Shared factor settings used by the staged notebooks and the local workbench.")
        st.dataframe(ADJUSTMENT_REFERENCE_DF, use_container_width=True, hide_index=True)
        if adjustments_df.empty:
            st.info("No municipio adjustment output is currently loaded into the local DuckDB workbench.")
        else:
            adjustment_columns = [
                "municipio",
                "child_rate",
                "elderly_65_plus_rate",
                "score_age_vulnerability",
                "age_adjustment_points",
                "vulnerability_score_base",
                "vulnerability_score_adjusted",
                "adjustment_config_version",
            ]
            adjustment_columns = [column for column in adjustment_columns if column in adjustments_df.columns]
            st.dataframe(
                adjustments_df[adjustment_columns],
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Vulnerability Factor Breakdown"):
        if factor_summary_df.empty:
            st.info("No stage-10 vulnerability factor output is currently loaded into the local DuckDB workbench.")
        else:
            factor_columns = [
                "municipio",
                "population",
                "poverty_rate",
                "no_vehicle_rate",
                "vacancy_rate",
                "poverty_score",
                "transport_constraint_score",
                "housing_fragility_score",
                "income_capacity_score",
                "vulnerability_score_base",
                "vulnerability_score_adjusted",
                "resilience_capacity_score",
            ]
            factor_columns = [column for column in factor_columns if column in factor_summary_df.columns]
            st.dataframe(
                factor_summary_df[factor_columns],
                use_container_width=True,
                hide_index=True,
            )

    with st.expander("Terrain Sidecar Review"):
        if terrain_summary_df.empty:
            st.info("No terrain summary output is currently loaded into the local DuckDB workbench.")
        else:
            terrain_columns = [
                "municipio_name",
                "terrain_data_completeness",
                "terrain_confidence_score",
                "elevation_mean",
                "slope_mean",
                "local_relief",
                "wetness_proxy",
                "distance_to_stream_km",
                "coastal_inundation_flag",
                "soil_runoff_potential",
                "land_cover_runoff_modifier",
            ]
            terrain_columns = [column for column in terrain_columns if column in terrain_summary_df.columns]
            st.dataframe(
                terrain_summary_df[terrain_columns],
                use_container_width=True,
                hide_index=True,
            )
finally:
    if con is not None:
        con.close()
