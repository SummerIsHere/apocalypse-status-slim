"""Offline HTML dashboard rendering."""

from __future__ import annotations

import json
from html import escape
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from apocalypse_status.config import AppConfig


@dataclass
class Section:
    slug: str
    title: str
    description: str
    charts_html: list[str]


@dataclass(frozen=True)
class SourceInfo:
    name: str
    dataset_description: str
    landing_url: str | None = None
    data_url: str | None = None
    latest_datasets: tuple[str, ...] | None = None
    show_latest: bool = True


def _source_catalog(config: AppConfig) -> dict[str, list[SourceInfo]]:
    source_cfg = config.sources
    nclim = source_cfg.get("nclimdiv", {})
    nclim_dir = str(nclim.get("ftp_dir", "pub/data/cirs/climdiv")).strip("/")
    nclim_url = f"https://www.ncei.noaa.gov/{nclim_dir}/"

    world_bank_cfg = source_cfg.get("world_bank", {})
    wb_country = str(world_bank_cfg.get("country", "WLD")).lower()
    wb_indicator = str(world_bank_cfg.get("indicator_id", "SP.POP.TOTL"))
    world_bank_data_url = (
        "https://api.worldbank.org/v2/country/"
        f"{wb_country}/indicator/{wb_indicator}?format=json&per_page=20000"
    )

    return {
        "co2": [
            SourceInfo(
                name="NOAA GML Mauna Loa CO2",
                dataset_description="Monthly atmospheric CO2 concentration (ppm) measured at Mauna Loa Observatory (record starts in 1958).",
                landing_url="https://gml.noaa.gov/ccgg/trends/",
                data_url=source_cfg.get("co2", {}).get("url"),
                latest_datasets=("co2",),
            ),
            SourceInfo(
                name="EPA Historical CO2 Composite (legacy ice-core context)",
                dataset_description=(
                    "Legacy historical CO2 context from an EPA composite of ice cores and stations "
                    "used in the prior dashboard."
                ),
                landing_url="https://www.epa.gov/climate-indicators/climate-change-indicators-atmospheric-concentrations-greenhouse-gases",
                data_url="https://www.epa.gov/sites/default/files/2016-08/ghg-concentrations_fig-1.csv",
                show_latest=False,
            ),
        ],
        "global_temp": [
            SourceInfo(
                name="Met Office HadCRUT",
                dataset_description="Monthly global near-surface (land + sea) temperature anomaly series.",
                landing_url="https://www.metoffice.gov.uk/hadobs/hadcrut5/",
                data_url=source_cfg.get("global_temp", {}).get("url"),
                latest_datasets=("global_temp",),
            )
        ],
        "nclimdiv": [
            SourceInfo(
                name="NOAA nClimDiv",
                dataset_description="U.S. climate division monthly temperature, precipitation, and degree-day aggregates (WA climate division 03 in this project).",
                landing_url="https://www.ncei.noaa.gov/",
                data_url=nclim_url,
                latest_datasets=("nclimdiv_tmpcdv", "nclimdiv_pcpndv", "nclimdiv_hddcdv", "nclimdiv_cddcdv"),
            )
        ],
        "snotel": [
            SourceInfo(
                name="USDA NRCS SNOTEL",
                dataset_description="Station snow water equivalent (SWE) observations used for Puget Sound snowpack trends, including April-1 style snowpack context.",
                landing_url="https://www.nrcs.usda.gov/wps/portal/wcc/home/",
                data_url="https://wcc.sc.egov.usda.gov/reportGenerator/",
                latest_datasets=("snotel_master",),
            )
        ],
        "streamflow": [
            SourceInfo(
                name="USGS NWIS",
                dataset_description="Daily streamflow discharge (cfs) by river station, aggregated to monthly flow-rate trends in this dashboard.",
                landing_url="https://waterdata.usgs.gov/nwis",
                data_url="https://waterservices.usgs.gov/docs/dv-service/daily-values-service-details/",
                latest_datasets=("streamflow_monthly", "streamflow_master"),
            )
        ],
        "tidal": [
            SourceInfo(
                name="NOAA CO-OPS",
                dataset_description="Monthly mean tide/sea-level observations from NWLON tide gauge stations.",
                landing_url="https://tidesandcurrents.noaa.gov/",
                data_url="https://tidesandcurrents.noaa.gov/api/",
                latest_datasets=("tidal_master", "annual_avg_msl"),
            )
        ],
        "world_bank": [
            SourceInfo(
                name="World Bank Indicator API",
                dataset_description="Global annual total population indicator (SP.POP.TOTL).",
                landing_url="https://data.worldbank.org/indicator/SP.POP.TOTL",
                data_url=world_bank_data_url,
                latest_datasets=("world_bank",),
            )
        ],
        "population_census_idb": [
            SourceInfo(
                name="US Census IDB API",
                dataset_description="Country-level population estimates aggregated to world totals for overlap/extension checks.",
                landing_url="https://www.census.gov/data-tools/demo/idb/",
                data_url=source_cfg.get("population_census_idb", {}).get("url"),
                latest_datasets=("population_census_idb",),
            )
        ],
        "grain": [
            SourceInfo(
                name="USDA PSD",
                dataset_description="Global grain production and total supply statistics used for grain-per-capita resource-depletion tracking.",
                landing_url="https://apps.fas.usda.gov/psdonline/app/index.html#/app/home",
                data_url=source_cfg.get("grain", {}).get("url"),
                latest_datasets=("grain", "grain_per_capita"),
            )
        ],
    }


def _chart_source_block_html(
    config: AppConfig,
    datasets: dict[str, pd.DataFrame],
    source_keys: list[str],
    extra_lines: list[str] | None = None,
) -> str:
    catalog = _source_catalog(config)
    lines: list[str] = []
    seen: set[str] = set()

    for key in source_keys:
        for source_info in catalog.get(key, []):
            dedupe_key = "|".join(
                [
                    source_info.name,
                    source_info.dataset_description,
                    source_info.landing_url or "",
                    source_info.data_url or "",
                ]
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            safe_name = escape(source_info.name)
            safe_desc = escape(source_info.dataset_description)
            link_parts: list[str] = []
            if source_info.landing_url:
                safe_landing = escape(source_info.landing_url, quote=True)
                link_parts.append(f"<a href='{safe_landing}' target='_blank' rel='noopener'>landing page</a>")
            if source_info.data_url:
                safe_data = escape(source_info.data_url, quote=True)
                link_parts.append(f"<a href='{safe_data}' target='_blank' rel='noopener'>data/API</a>")
            links_html = f" ({' | '.join(link_parts)})" if link_parts else ""
            lines.append(f"<li><strong>{safe_name}:</strong> {safe_desc}{links_html}</li>")

    if "combined_co2" in datasets and "co2" in source_keys:
        combined = datasets["combined_co2"]
        if "Source" in combined.columns:
            unique_sources = sorted({str(val).strip() for val in combined["Source"].dropna().tolist() if str(val).strip()})
            if unique_sources:
                lines.append(f"<li><strong>Combined CO2 components:</strong> {escape(', '.join(unique_sources))}</li>")

    if extra_lines:
        for line in extra_lines:
            lines.append(f"<li>{escape(line)}</li>")

    if not lines:
        lines.append("<li>Source metadata unavailable.</li>")

    return (
        "<div class='chart-source'>"
        "<h4>Data comes from</h4>"
        f"<ul>{''.join(lines)}</ul>"
        "</div>"
    )


def _wrap_chart_with_sources(chart_html: str, source_html: str) -> str:
    return f"<div class='chart-wrap'>{chart_html}{source_html}</div>"


def _line_with_refs(
    frame: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    refs: list[tuple[float, str]] | None = None,
) -> go.Figure:
    fig = px.line(frame, x=x, y=y, title=title)
    refs = refs or []
    for y_value, label in refs:
        fig.add_hline(y=y_value, line_dash="dot", annotation_text=label, annotation_position="top left")
    fig.update_layout(template="plotly_white", margin=dict(l=30, r=20, t=50, b=30))
    return fig


def _fig_to_html(fig: go.Figure, include_plotly_js: bool) -> str:
    return pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs="inline" if include_plotly_js else False,
        config={"responsive": True},
    )


def _metric_cards(datasets: dict[str, pd.DataFrame]) -> str:
    cards: list[tuple[str, str]] = []

    co2 = datasets.get("co2")
    if co2 is not None and not co2.empty:
        latest = co2.sort_values("Date").iloc[-1]
        cards.append(("Latest CO2 ppm", f"{latest['avg CO2 ppm']:.2f}"))

    temp = datasets.get("global_temp")
    if temp is not None and not temp.empty:
        latest = temp.sort_values("Date").iloc[-1]
        cards.append(("Latest Global Temp Deviation (C)", f"{latest['Median Global Temp C Deviation']:.2f}"))

    sf = datasets.get("streamflow_monthly")
    if sf is not None and not sf.empty:
        provisional_months = int(sf["is_provisional_month"].sum())
        cards.append(("Provisional Streamflow Months", str(provisional_months)))

    sea = datasets.get("annual_avg_msl")
    if sea is not None and not sea.empty:
        latest = sea.sort_values("Year").iloc[-1]
        cards.append(("Sea Level Change Since 2000 (in)", f"{latest['Change in Mean Sea Level since 2000 (in)']:.2f}"))

    card_html = []
    for title, value in cards:
        card_html.append(
            f"<div class='metric-card'><div class='metric-title'>{title}</div><div class='metric-value'>{value}</div></div>"
        )

    return "\n".join(card_html)


def _dataset_max_date(frame: pd.DataFrame) -> pd.Timestamp | None:
    date_candidates = [col for col in frame.columns if "date" in col.lower() or col.lower().endswith("_dt")]
    for col in date_candidates:
        try:
            parsed = pd.to_datetime(frame[col], errors="coerce", format="mixed")
        except TypeError:
            parsed = pd.to_datetime(frame[col], errors="coerce")
        if parsed.notna().any():
            return parsed.max()

    # Handle climate-division style datasets with YEAR + Month columns.
    if {"YEAR", "Month"}.issubset(frame.columns):
        ym_frame = frame
        if "value" in frame.columns:
            value_mask = pd.to_numeric(frame["value"], errors="coerce").notna()
            ym_frame = frame[value_mask]
        years = pd.to_numeric(ym_frame["YEAR"], errors="coerce")
        months = pd.to_numeric(ym_frame["Month"], errors="coerce")
        series = pd.to_datetime(
            years.astype("Int64").astype(str)
            + "-"
            + months.astype("Int64").astype(str).str.zfill(2)
            + "-01",
            errors="coerce",
        )
        if series.notna().any():
            return series.max()

    if {"year", "month"}.issubset(frame.columns):
        years = pd.to_numeric(frame["year"], errors="coerce")
        months = pd.to_numeric(frame["month"], errors="coerce")
        series = pd.to_datetime(
            years.astype("Int64").astype(str)
            + "-"
            + months.astype("Int64").astype(str).str.zfill(2)
            + "-01",
            errors="coerce",
        )
        if series.notna().any():
            return series.max()

    if {"year_nu", "month_nu"}.issubset(frame.columns):
        series = pd.to_datetime(
            frame["year_nu"].astype("Int64").astype(str)
            + "-"
            + frame["month_nu"].astype("Int64").astype(str).str.zfill(2)
            + "-01",
            errors="coerce",
        )
        if series.notna().any():
            return series.max()

    for year_col in ("Year", "YEAR", "Market_Year", "market_year"):
        if year_col in frame.columns:
            years = pd.to_numeric(frame[year_col], errors="coerce").dropna()
            if not years.empty:
                return pd.to_datetime(f"{int(years.max())}-01-01")

    if "Year" in frame.columns:
        years = pd.to_numeric(frame["Year"], errors="coerce").dropna()
        if not years.empty:
            return pd.to_datetime(f"{int(years.max())}-01-01")

    return None


def _latest_for_source(
    source_key: str,
    datasets: dict[str, pd.DataFrame],
    preferred_datasets: tuple[str, ...] | None = None,
) -> pd.Timestamp | None:
    dataset_map: dict[str, list[str]] = {
        "co2": ["co2"],
        "global_temp": ["global_temp"],
        "nclimdiv": ["nclimdiv_tmpcdv", "nclimdiv_pcpndv", "nclimdiv_hddcdv", "nclimdiv_cddcdv"],
        "snotel": ["snotel_master"],
        "streamflow": ["streamflow_monthly", "streamflow_master"],
        "tidal": ["tidal_master", "annual_avg_msl"],
        "world_bank": ["world_bank", "population_combined"],
        "population_census_idb": ["population_census_idb", "population_combined"],
        "grain": ["grain", "grain_per_capita"],
    }
    candidates = list(preferred_datasets) if preferred_datasets else dataset_map.get(source_key, [source_key])
    maxima: list[pd.Timestamp] = []
    for dataset_name in candidates:
        frame = datasets.get(dataset_name)
        if frame is None or frame.empty:
            continue
        max_dt = _dataset_max_date(frame)
        if max_dt is None or pd.isna(max_dt):
            continue
        maxima.append(pd.Timestamp(max_dt))
    if not maxima:
        return None
    return max(maxima)


def _source_refresh_panel_html(config: AppConfig, datasets: dict[str, pd.DataFrame]) -> str:
    now = pd.Timestamp.now(tz="UTC")
    catalog = _source_catalog(config)
    source_order = [key for key in config.sources.keys() if key in catalog]
    for key in catalog:
        if key not in source_order:
            source_order.append(key)

    rows: list[str] = []
    for source_key in source_order:
        entries = catalog.get(source_key, [])
        if not entries:
            continue
        for info in entries:
            if info.show_latest:
                max_dt = _latest_for_source(source_key, datasets, preferred_datasets=info.latest_datasets)
            else:
                max_dt = None
            if max_dt is None:
                latest = "n/a"
                age = "n/a"
            else:
                max_dt_utc = max_dt.tz_localize("UTC") if max_dt.tzinfo is None else max_dt
                latest = max_dt_utc.strftime("%Y-%m-%d")
                age = str(int((now - max_dt_utc).days))

            links: list[str] = []
            if info.landing_url:
                safe_landing = escape(info.landing_url, quote=True)
                links.append(f"<a href='{safe_landing}' target='_blank' rel='noopener'>landing page</a>")
            if info.data_url:
                safe_data = escape(info.data_url, quote=True)
                links.append(f"<a href='{safe_data}' target='_blank' rel='noopener'>data/API</a>")

            rows.append(
                "<tr>"
                f"<td>{escape(source_key)}</td>"
                f"<td><strong>{escape(info.name)}:</strong> {escape(info.dataset_description)}</td>"
                f"<td>{latest}</td>"
                f"<td>{age}</td>"
                f"<td>{' | '.join(links) if links else 'n/a'}</td>"
                "</tr>"
            )

    return (
        "<div class='freshness-wrap'>"
        "<h3>Data Source Refresh</h3>"
        "<table class='freshness-table data-source-table'>"
        "<colgroup>"
        "<col class='col-source' />"
        "<col class='col-dataset' />"
        "<col class='col-latest' />"
        "<col class='col-age' />"
        "<col class='col-links' />"
        "</colgroup>"
        "<thead><tr><th>Source</th><th>Dataset</th><th>Latest Date</th><th>Age (days)</th><th>Links</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</div>"
    )


def _build_streamflow_seasonality(
    streamflow: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, str, str] | None:
    sf = streamflow.copy()
    sf["year_nu"] = pd.to_numeric(sf["year_nu"], errors="coerce")
    sf["month_nu"] = pd.to_numeric(sf["month_nu"], errors="coerce")
    sf["monthly_cfs"] = pd.to_numeric(sf["monthly_cfs"], errors="coerce")
    sf = sf.dropna(subset=["year_nu", "month_nu", "monthly_cfs"])
    sf = sf[(sf["month_nu"] >= 1) & (sf["month_nu"] <= 12)]

    if "month_completeness" in sf.columns:
        completeness = pd.to_numeric(sf["month_completeness"], errors="coerce")
        sf = sf[completeness.fillna(0.0) >= 0.7]

    if sf.empty or sf["year_nu"].nunique() < 5:
        return None

    sf["year_nu"] = sf["year_nu"].astype(int)
    sf["month_nu"] = sf["month_nu"].astype(int)
    max_year = int(sf["year_nu"].max())
    recent_start = max_year - 9
    recent = sf[sf["year_nu"].between(recent_start, max_year)].copy()

    baseline = sf[sf["year_nu"].between(1981, 2010)].copy()
    if baseline["year_nu"].nunique() < 10:
        all_years = sorted(sf["year_nu"].unique())
        cutoff = max(0, len(all_years) - 10)
        baseline_years = all_years[:cutoff] if cutoff > 0 else all_years
        baseline = sf[sf["year_nu"].isin(baseline_years)].copy()

    if baseline.empty or recent.empty:
        return None

    annual_total = (
        sf.groupby(["site_no", "year_nu"], as_index=False)["monthly_cfs"]
        .sum()
        .rename(columns={"monthly_cfs": "annual_total_cfs"})
    )
    sf = sf.merge(annual_total, on=["site_no", "year_nu"], how="left")
    sf = sf[sf["annual_total_cfs"] > 0]
    sf["seasonal_share"] = sf["monthly_cfs"] / sf["annual_total_cfs"]

    baseline_curves = (
        sf[sf["year_nu"].isin(baseline["year_nu"].unique())]
        .groupby(["site_no", "River Name", "River Type", "month_nu"], as_index=False)["seasonal_share"]
        .mean()
        .rename(columns={"seasonal_share": "baseline_share"})
    )
    recent_curves = (
        sf[sf["year_nu"].isin(recent["year_nu"].unique())]
        .groupby(["site_no", "River Name", "River Type", "month_nu"], as_index=False)["seasonal_share"]
        .mean()
        .rename(columns={"seasonal_share": "recent_share"})
    )

    if baseline_curves.empty or recent_curves.empty:
        return None

    combined = baseline_curves.merge(
        recent_curves,
        on=["site_no", "River Name", "River Type", "month_nu"],
        how="inner",
    )
    if combined.empty:
        return None

    type_lines = pd.concat(
        [
            combined[["River Type", "month_nu", "baseline_share"]]
            .rename(columns={"baseline_share": "seasonal_share"})
            .assign(Period=f"Baseline ({int(baseline['year_nu'].min())}-{int(baseline['year_nu'].max())})"),
            combined[["River Type", "month_nu", "recent_share"]]
            .rename(columns={"recent_share": "seasonal_share"})
            .assign(Period=f"Recent ({recent_start}-{max_year})"),
        ],
        ignore_index=True,
    )

    delta = combined.copy()
    delta["pct_change_vs_baseline"] = ((delta["recent_share"] - delta["baseline_share"]) / delta["baseline_share"]) * 100.0
    delta.loc[delta["baseline_share"] <= 0, "pct_change_vs_baseline"] = pd.NA
    delta["pct_change_vs_baseline"] = delta["pct_change_vs_baseline"].clip(-200, 200)
    delta["River Label"] = (
        delta["River Name"].fillna(delta["site_no"].astype(str))
        + " ("
        + delta["River Type"].fillna("Unknown")
        + ")"
    )

    baseline_label = f"{int(baseline['year_nu'].min())}-{int(baseline['year_nu'].max())}"
    recent_label = f"{recent_start}-{max_year}"
    return type_lines, delta, baseline_label, recent_label


def _top_river_types(streamflow: pd.DataFrame, limit: int = 3) -> list[str]:
    frame = streamflow.copy()
    frame["River Type"] = frame["River Type"].fillna("Unknown").astype(str)
    if "site_no" in frame.columns:
        counts = frame.groupby("River Type")["site_no"].nunique()
    else:
        counts = frame.groupby("River Type").size()
    return counts.sort_values(ascending=False).index.tolist()[:limit]


def _build_snowpack_views(
    monthly: pd.DataFrame,
    swe_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, str, list[str]]:
    prepared = monthly.copy().sort_values("Date").reset_index(drop=True)
    prepared["SWE 5 Yr Rolling Mean (in)"] = prepared[swe_col].rolling(window=60, min_periods=60).mean()
    prepared["SWE 30 Yr Rolling Mean (in)"] = prepared[swe_col].rolling(window=360, min_periods=360).mean()

    prepared["water_month"] = prepared["Date"].dt.month.astype(int)
    prepared["water_year"] = prepared["Date"].dt.year + (prepared["water_month"] >= 10).astype(int)
    prepared["water_month_order"] = ((prepared["water_month"] + 2) % 12) + 1
    month_labels = ["Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep"]
    month_label_map = {idx + 1: label for idx, label in enumerate(month_labels)}
    prepared["Water Month"] = prepared["water_month_order"].map(month_label_map)

    counts = prepared.groupby("water_year", as_index=False)["water_month_order"].nunique()
    complete_years = sorted(counts[counts["water_month_order"] == 12]["water_year"].astype(int).tolist())
    available_years = sorted(prepared["water_year"].dropna().astype(int).unique().tolist())

    target_baseline = list(range(1981, 2011))
    if complete_years and all(year in complete_years for year in target_baseline):
        baseline_years = target_baseline
    elif len(complete_years) >= 30:
        baseline_years = complete_years[:30]
    elif complete_years:
        baseline_years = complete_years
    else:
        baseline_years = available_years[:30]

    if len(complete_years) >= 10:
        recent_years = complete_years[-10:]
    elif complete_years:
        recent_years = complete_years
    else:
        recent_years = available_years[-10:]

    baseline_label = (
        f"{min(baseline_years)}-{max(baseline_years)}"
        if baseline_years
        else "Baseline unavailable"
    )
    recent_label = (
        f"{min(recent_years)}-{max(recent_years)}"
        if recent_years
        else "Recent unavailable"
    )

    profile_frames: list[pd.DataFrame] = []
    if baseline_years:
        baseline = (
            prepared[prepared["water_year"].isin(baseline_years)]
            .groupby(["water_month_order", "Water Month"], as_index=False)[swe_col]
            .median()
            .assign(Period=f"Baseline ({baseline_label})")
        )
        profile_frames.append(baseline)
    if recent_years:
        recent = (
            prepared[prepared["water_year"].isin(recent_years)]
            .groupby(["water_month_order", "Water Month"], as_index=False)[swe_col]
            .median()
            .assign(Period=f"Recent ({recent_label})")
        )
        profile_frames.append(recent)

    profile = pd.concat(profile_frames, ignore_index=True) if profile_frames else pd.DataFrame()

    annual_peak = (
        prepared.groupby("water_year", as_index=False)[swe_col]
        .max()
        .rename(columns={"water_year": "Water Year", swe_col: "Annual Peak SWE (in)"})
        .sort_values("Water Year")
        .reset_index(drop=True)
    )
    annual_peak["Annual Peak SWE 5 Yr Rolling Mean (in)"] = annual_peak["Annual Peak SWE (in)"].rolling(
        window=5, min_periods=5
    ).mean()

    return prepared, profile, annual_peak, baseline_label, recent_label, month_labels


def _load_snotel_region_groups(config: AppConfig) -> pd.DataFrame:
    source_cfg = config.sources.get("snotel", {})
    stations_csv = source_cfg.get("stations_csv")
    if not stations_csv:
        return pd.DataFrame(columns=["snotel_station_id_norm", "snotel_region_group"])

    path = Path(str(stations_csv)).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    if not path.exists():
        return pd.DataFrame(columns=["snotel_station_id_norm", "snotel_region_group"])

    stations = pd.read_csv(path)
    if "snotel_station_id" in stations.columns:
        station_ids = (
            stations["snotel_station_id"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0+$", "", regex=True)
        )
    elif "station_triplet" in stations.columns:
        station_ids = stations["station_triplet"].astype(str).str.strip().str.split(":").str[0]
    else:
        return pd.DataFrame(columns=["snotel_station_id_norm", "snotel_region_group"])

    if "legacy_regions" in stations.columns:
        region_text = stations["legacy_regions"].fillna("").astype(str)
    elif "state_sub-basin_name" in stations.columns:
        region_text = stations["state_sub-basin_name"].fillna("").astype(str)
    else:
        region_text = pd.Series([""] * len(stations), index=stations.index)

    wria_ids = (
        pd.to_numeric(stations["wria_id"], errors="coerce")
        if "wria_id" in stations.columns
        else pd.Series([pd.NA] * len(stations), index=stations.index, dtype="object")
    )
    wria_known = wria_ids.notna()
    wria_puget = wria_ids.between(1, 19, inclusive="both")

    grouped_value = (
        stations["region_group"].fillna("").astype(str).str.strip()
        if "region_group" in stations.columns
        else pd.Series([""] * len(stations), index=stations.index)
    )
    grouped_known = grouped_value.isin(["Puget Sound", "Other WA"])
    grouped_puget = grouped_value.eq("Puget Sound")

    legacy_puget = region_text.str.contains("PUGET SOUND", case=False, regex=False)
    resolved = pd.Series(False, index=stations.index)
    is_puget = pd.Series(False, index=stations.index)

    is_puget = is_puget.mask(wria_known, wria_puget)
    resolved = resolved | wria_known
    is_puget = is_puget.mask(~resolved & grouped_known, grouped_puget)
    resolved = resolved | grouped_known
    is_puget = is_puget.mask(~resolved, legacy_puget)

    tagged = pd.DataFrame(
        {
            "snotel_station_id_norm": station_ids,
            "is_puget": is_puget,
        }
    )
    station_token = tagged["snotel_station_id_norm"].astype(str).str.strip().str.lower()
    tagged = tagged[~station_token.isin({"", "nan", "none"})]
    grouped = tagged.groupby("snotel_station_id_norm", as_index=False)["is_puget"].max()
    grouped["snotel_region_group"] = grouped["is_puget"].map({True: "Puget Sound", False: "Other WA"})
    return grouped[["snotel_station_id_norm", "snotel_region_group"]]


def build_sections(config: AppConfig, datasets: dict[str, pd.DataFrame]) -> list[Section]:
    sections: list[Section] = []
    include_js = True

    def add_section(
        title: str,
        description: str,
        figs: list[go.Figure],
        slug: str,
        source_keys: list[str],
        extra_source_lines: list[str] | None = None,
    ) -> None:
        nonlocal include_js
        html_parts: list[str] = []
        source_block = _chart_source_block_html(config, datasets, source_keys, extra_source_lines)
        for fig in figs:
            chart_html = _fig_to_html(fig, include_plotly_js=include_js)
            html_parts.append(_wrap_chart_with_sources(chart_html, source_block))
            include_js = False
        sections.append(Section(slug=slug, title=title, description=description, charts_html=html_parts))

    def add_streamflow_grid_section(title: str, description: str, figs: list[go.Figure], slug: str) -> None:
        nonlocal include_js
        cards: list[str] = []
        for fig in figs:
            fig.update_layout(
                legend=dict(orientation="h", yanchor="top", y=-0.22, xanchor="left", x=0.0),
                margin=dict(l=30, r=20, t=55, b=90),
            )
            chart_html = _fig_to_html(fig, include_plotly_js=include_js)
            cards.append(f"<div class='streamflow-grid-card'>{chart_html}</div>")
            include_js = False
        source_block = _chart_source_block_html(config, datasets, ["streamflow"])
        sections.append(
            Section(
                slug=slug,
                title=title,
                description=description,
                charts_html=[f"<div class='streamflow-grid'>{''.join(cards)}</div>{source_block}"],
            )
        )

    combined_co2 = datasets.get("combined_co2")
    if combined_co2 is not None and not combined_co2.empty:
        fig1 = _line_with_refs(
            combined_co2,
            x="Year",
            y="CO2 ppm",
            title="Combined CO2 Trend",
            refs=[(350, "Hansen Safe Line"), (415, "Pliocene Danger Threshold")],
        )
        add_section(
            "Climate Change - CO2 Levels",
            "Historic plus modern CO2 concentration.",
            [fig1],
            "co2",
            source_keys=["co2"],
            extra_source_lines=["Includes ice-core values from bundled historical archive when available."],
        )

    global_temp = datasets.get("global_temp")
    if global_temp is not None and not global_temp.empty:
        fig2 = _line_with_refs(
            global_temp.sort_values("Date"),
            x="Date",
            y="Median Global Temp C Deviation",
            title="Global Temperature Deviation",
            refs=[(1.5, "Paris Climate Agreement Goal"), (3.0, "High-risk threshold")],
        )
        add_section(
            "Climate Change - Temperature Deviation",
            "Monthly global temperature anomaly series.",
            [fig2],
            "temp-dev",
            source_keys=["global_temp"],
        )

    grain_pc = datasets.get("grain_per_capita")
    if grain_pc is not None and not grain_pc.empty:
        fig3 = _line_with_refs(grain_pc, x="Date", y="kg Grain Per Capita", title="Grain Per Capita")
        add_section(
            "Resource Depletion",
            "Global grain supply per capita trend.",
            [fig3],
            "resource",
            source_keys=["grain", "world_bank", "population_census_idb"],
        )

    wa_temp = datasets.get("wa03_annual_temp")
    if wa_temp is not None and not wa_temp.empty:
        fig4 = px.line(
            wa_temp,
            x="RPT_MO_STRT_DT",
            y=["Temp F Deviation", "5 yr avg", "Temp F Deviation rolling average"],
            title="Puget Sound Annual Temperature Deviation",
        )
        fig4.update_layout(template="plotly_white")
        add_section(
            "Puget Sound Temperature Deviation",
            "Puget Sound annual temperature deviations.",
            [fig4],
            "ps-temp",
            source_keys=["nclimdiv"],
        )

    notes_cards = _metric_cards(datasets)
    source_refresh_panel = _source_refresh_panel_html(config, datasets)
    sections.append(
        Section(
            slug="notes",
            title="Data Quality",
            description="Current snapshot metrics and source/data quality notes.",
            charts_html=[f"<div class='metric-grid'>{notes_cards}</div>{source_refresh_panel}"],
        )
    )

    wa_precip = datasets.get("wa03_precip")
    if wa_precip is not None and not wa_precip.empty:
        wa_precip_plot = wa_precip.copy()
        seasons = ("Spring", "Summer", "Fall", "Winter")
        for season in seasons:
            dev_col = f"{season} Precip % Dev"
            if dev_col not in wa_precip_plot.columns:
                continue
            wa_precip_plot[f"{season} Precip % Dev 5 Yr Avg Full"] = (
                pd.to_numeric(wa_precip_plot[dev_col], errors="coerce").rolling(window=5, min_periods=5).mean()
            )
            wa_precip_plot[f"{season} Precip % Dev 30 Yr Avg Full"] = (
                pd.to_numeric(wa_precip_plot[dev_col], errors="coerce").rolling(window=30, min_periods=30).mean()
            )

        fig5 = px.line(
            wa_precip_plot,
            x="Precip YearDate",
            y=[
                "Spring Precip % Dev 5 Yr Avg Full",
                "Summer Precip % Dev 5 Yr Avg Full",
                "Fall Precip % Dev 5 Yr Avg Full",
                "Winter Precip % Dev 5 Yr Avg Full",
            ],
            title="Puget Sound Precipitation Deviation (5Y Avg)",
        )
        fig5.update_layout(template="plotly_white")
        add_section(
            "Puget Sound Precipitation Deviation (5Y Avg)",
            "Seasonal precipitation deviation trends using a fixed 1951-1980 baseline. "
            "5-year rolling means begin only after 5 full years of data.",
            [fig5],
            "ps-precip-dev",
            source_keys=["nclimdiv"],
            extra_source_lines=[
                "Fixed baseline: each season's percent deviation is computed relative to the 1951-1980 seasonal mean.",
                "This chart uses full 5-year windows; early years are intentionally omitted until enough data exists.",
            ],
        )

        fig6 = px.line(
            wa_precip_plot,
            x="Precip YearDate",
            y=[
                "Spring Precip % Dev 30 Yr Avg Full",
                "Summer Precip % Dev 30 Yr Avg Full",
                "Fall Precip % Dev 30 Yr Avg Full",
                "Winter Precip % Dev 30 Yr Avg Full",
            ],
            title="Puget Sound Precipitation Deviation (30Y Avg)",
        )
        fig6.update_layout(template="plotly_white")
        add_section(
            "Puget Sound Precipitation Deviation (30Y Avg)",
            "Long-run precipitation context using a fixed 1951-1980 baseline. "
            "30-year rolling means begin only after 30 full years of data.",
            [fig6],
            "ps-dd-dev",
            source_keys=["nclimdiv"],
            extra_source_lines=[
                "Fixed baseline: each season's percent deviation is computed relative to the 1951-1980 seasonal mean.",
                "This chart uses full 30-year windows; early years are intentionally omitted until enough data exists.",
            ],
        )

        fig7 = px.area(
            wa_precip,
            x="Precip YearDate",
            y=[
                "Spring Precipitation Inches",
                "Summer Precipitation Inches",
                "Fall Precipitation Inches",
                "Winter Precipitation Inches",
            ],
            title="PS Annual Precipitation",
        )
        fig7.update_layout(template="plotly_white")
        add_section(
            "Puget Sound Annual Precipitation",
            "Seasonal precipitation totals.",
            [fig7],
            "ps-annual-precip",
            source_keys=["nclimdiv"],
        )

    wa_dd = datasets.get("wa03_degree_days")
    if wa_dd is not None and not wa_dd.empty:
        fig8 = px.line(
            wa_dd,
            x="DD Year Date",
            y=["Annual Heating Degree Days", "Annual Heating Degree Days 5 Yrs Avg", "Annual Heating Degree Days 30 Yrs Avg"],
            title="Puget Sound Degree Days - Heating",
        )
        fig8.update_layout(template="plotly_white")

        fig9 = px.line(
            wa_dd,
            x="DD Year Date",
            y=["Annual Cooling Degree Days", "Annual Cooling Degree Days 5 Yrs Avg", "Annual Cooling Degree Days 30 Yrs Avg"],
            title="Puget Sound Degree Days - Cooling",
        )
        fig9.update_layout(template="plotly_white")
        add_section(
            "Puget Sound Degree Days",
            "Annual heating/cooling degree day trends.",
            [fig8, fig9],
            "ps-dd",
            source_keys=["nclimdiv"],
        )

    if wa_temp is not None and not wa_temp.empty:
        fig10 = _line_with_refs(wa_temp, x="RPT_MO_STRT_DT", y="Avg Annual Temp F", title="Puget Sound Annual Air Temp")
        add_section(
            "Puget Sound Annual Air Temp",
            "Annual average temperature for WA climate division 03.",
            [fig10],
            "ps-air-temp",
            source_keys=["nclimdiv"],
        )

    snotel = datasets.get("snotel_master")
    if snotel is not None and not snotel.empty:
        swe_col = [c for c in snotel.columns if "Snow Water Equivalent" in c and "Values" in c]
        if swe_col:
            col = swe_col[0]
            snow = snotel.copy()
            snow["Date"] = pd.to_datetime(snow["Date"], errors="coerce")
            snow[col] = pd.to_numeric(snow[col], errors="coerce")
            snow = snow.dropna(subset=["Date", col])
            snow["snotel_region_group"] = "Other WA"
            region_groups = _load_snotel_region_groups(config)
            if not region_groups.empty and "snotel_station_id" in snow.columns:
                snow["snotel_station_id_norm"] = (
                    snow["snotel_station_id"]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\.0+$", "", regex=True)
                )
                snow = snow.merge(region_groups, on="snotel_station_id_norm", how="left", suffixes=("", "_mapped"))
                snow["snotel_region_group"] = snow["snotel_region_group_mapped"].fillna("Other WA")
                snow = snow.drop(columns=["snotel_region_group_mapped"], errors="ignore")
            # Group by explicit month-end timestamps so Date remains a concrete plotting column.
            snow["Date"] = snow["Date"].dt.to_period("M").dt.to_timestamp("M")
            puget_only = snow[snow["snotel_region_group"] == "Puget Sound"].copy()
            monthly_source = puget_only if not puget_only.empty else snow
            monthly = (
                monthly_source.groupby("Date", as_index=False)[col]
                .mean()
                .sort_values("Date")
                .reset_index(drop=True)
            )
            monthly, seasonal_profile, annual_peak, baseline_label, recent_label, water_month_labels = _build_snowpack_views(
                monthly, col
            )

            monthly_by_group = (
                snow.groupby(["Date", "snotel_region_group"], as_index=False)[col]
                .mean()
                .sort_values("Date")
                .reset_index(drop=True)
            )
            if monthly_by_group["snotel_region_group"].nunique() > 1:
                fig11 = px.line(
                    monthly_by_group,
                    x="Date",
                    y=col,
                    color="snotel_region_group",
                    title="Puget Sound Snowpack Levels",
                )
                fig11.update_layout(template="plotly_white", margin=dict(l=30, r=20, t=50, b=30))
            else:
                fig11 = _line_with_refs(monthly, x="Date", y=col, title="Puget Sound Snowpack Levels")
            fig12 = px.line(
                x="Date",
                data_frame=monthly,
                y=["SWE 5 Yr Rolling Mean (in)", "SWE 30 Yr Rolling Mean (in)"],
                title="Puget Sound Snowpack Rolling Means (Absolute SWE)",
            )
            fig12.update_layout(
                template="plotly_white",
                legend=dict(orientation="h", yanchor="top", y=-0.22, xanchor="left", x=0.0),
                margin=dict(l=30, r=20, t=50, b=90),
            )

            add_section(
                "Puget Sound Snowpack Levels",
                (
                    "Monthly mean snow water equivalent (absolute inches), grouped by legacy region "
                    "(Puget Sound vs Other WA). Puget-specific summary panels below use only Puget Sound stations."
                ),
                [fig11],
                "ps-snowpack-level",
                source_keys=["snotel"],
            )
            add_section(
                "Puget Sound Snowpack Rolling Means",
                "Full-window trailing 5-year and 30-year SWE rolling means (absolute inches).",
                [fig12],
                "ps-snowpack-rolling",
                source_keys=["snotel"],
            )

            if not seasonal_profile.empty:
                fig13 = px.line(
                    seasonal_profile,
                    x="Water Month",
                    y=col,
                    color="Period",
                    category_orders={"Water Month": water_month_labels},
                    markers=True,
                    title="Puget Sound Snowpack Seasonal Profile (Median SWE by Water Month)",
                )
                fig13.update_layout(template="plotly_white")
                add_section(
                    "Puget Sound Snowpack Seasonal Profile",
                    f"Median SWE seasonality comparison. Baseline={baseline_label}; Recent={recent_label}.",
                    [fig13],
                    "ps-snowpack-seasonal",
                    source_keys=["snotel"],
                )

            if not annual_peak.empty:
                fig14 = px.line(
                    annual_peak,
                    x="Water Year",
                    y=["Annual Peak SWE (in)", "Annual Peak SWE 5 Yr Rolling Mean (in)"],
                    title="Puget Sound Snowpack Annual Peak SWE",
                )
                fig14.update_layout(template="plotly_white")
                add_section(
                    "Puget Sound Snowpack Annual Peak SWE",
                    "Annual peak SWE by water year with 5-year rolling mean.",
                    [fig14],
                    "ps-snowpack-annual-peak",
                    source_keys=["snotel"],
                )

    sf = datasets.get("streamflow_master")
    if sf is not None and not sf.empty:
        sf_plot = sf.copy()
        sf_plot["Date"] = pd.to_datetime(
            sf_plot["year_nu"].astype(int).astype(str)
            + "-"
            + sf_plot["month_nu"].astype(int).astype(str).str.zfill(2)
            + "-01"
        )
        sf_plot["River Type"] = sf_plot["River Type"].fillna("Unknown")
        seasonality = _build_streamflow_seasonality(sf_plot)
        if seasonality is None:
            fig_overview = px.line(
                sf_plot,
                x="Date",
                y="monthly_cfs",
                color="data_status",
                line_group="site_no",
                hover_data=["site_no", "River Name", "River Type", "month_completeness"],
                title="All River Types",
            )
            fig_overview.update_layout(template="plotly_white")

            type_figs: list[go.Figure] = [fig_overview]
            for river_type in _top_river_types(sf_plot, limit=3):
                type_sf = sf_plot[sf_plot["River Type"] == river_type].copy()
                if type_sf.empty:
                    continue
                fig_type = px.line(
                    type_sf,
                    x="Date",
                    y="monthly_cfs",
                    color="data_status",
                    line_group="site_no",
                    hover_data=["site_no", "River Name", "River Type", "month_completeness"],
                    title=f"River Type: {river_type}",
                )
                fig_type.update_layout(template="plotly_white")
                type_figs.append(fig_type)

            add_streamflow_grid_section(
                "River Streamflows",
                "Seasonal Hydrograph Shape: unavailable for this dataset (insufficient history), so monthly flows are shown by river type.",
                type_figs,
                "river-streamflow",
            )
        else:
            month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            type_lines, delta, baseline_label, recent_label = seasonality
            type_lines["Month"] = pd.to_datetime(type_lines["month_nu"], format="%m", errors="coerce").dt.strftime("%b")
            type_lines["River Type"] = type_lines["River Type"].fillna("Unknown")
            type_plot = (
                type_lines.groupby(["River Type", "month_nu", "Month", "Period"], as_index=False)["seasonal_share"].mean()
            )

            fig_overview = px.line(
                type_plot,
                x="Month",
                y="seasonal_share",
                color="River Type",
                line_dash="Period",
                category_orders={"Month": month_labels},
                markers=True,
                title="All River Types",
            )
            fig_overview.update_layout(template="plotly_white", yaxis_tickformat=".0%")

            type_figs: list[go.Figure] = [fig_overview]
            for river_type in _top_river_types(type_plot, limit=3):
                type_specific = type_plot[type_plot["River Type"] == river_type].copy()
                if type_specific.empty:
                    continue
                fig_type_line = px.line(
                    type_specific,
                    x="Month",
                    y="seasonal_share",
                    color="Period",
                    category_orders={"Month": month_labels},
                    markers=True,
                    title=f"River Type: {river_type}",
                )
                fig_type_line.update_layout(template="plotly_white", yaxis_tickformat=".0%")
                type_figs.append(fig_type_line)

            add_streamflow_grid_section(
                "River Streamflows",
                (
                    f"Seasonal Hydrograph Shape by river type. Baseline={baseline_label}, Recent={recent_label}."
                ),
                type_figs,
                "river-streamflow",
            )

    sea = datasets.get("annual_avg_msl")
    if sea is not None and not sea.empty:
        fig17 = _line_with_refs(
            sea,
            x="Year Date",
            y="Change in Mean Sea Level since 2000 (in)",
            title="Change in Mean Sea Level",
            refs=[(-1, "2050 Lower Bound"), (7, "2050 Avg"), (19, "2050 Higher Bound"), (3, "2030 Avg"), (9, "2030 Higher Bound")],
        )
        fig18 = _line_with_refs(
            sea,
            x="Year Date",
            y="Change in Mean Sea Level since 2000 (in)",
            title="Overall Change in Mean Sea Level",
            refs=[(-1, "2050 Lower Bound"), (7, "2050 Avg"), (19, "2050 Higher Bound")],
        )
        add_section(
            "Change in Mean Sea Level",
            "Annual average MSL change from STND datum baseline.",
            [fig17],
            "sea-level",
            source_keys=["tidal"],
        )
        add_section(
            "Overall Change in Mean Sea Level",
            "Condensed sea level trend view.",
            [fig18],
            "sea-level-overall",
            source_keys=["tidal"],
        )

    return sections


def generate_dashboard_html(config: AppConfig, datasets: dict[str, pd.DataFrame], output_path: Path) -> Path:
    sections = build_sections(config, datasets)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    nav_items = "\n".join(
        [
            f"<button class='tab-button {'active' if idx == 0 else ''}' data-target='{section.slug}'>{section.title}</button>"
            for idx, section in enumerate(sections)
        ]
    )

    panels = "\n".join(
        [
            "".join(
                [
                    f"<section id='{section.slug}' class='tab-panel {'active' if idx == 0 else ''}'>",
                    f"<h2>{section.title}</h2>",
                    f"<p class='section-desc'>{section.description}</p>",
                    "\n".join(section.charts_html),
                    "</section>",
                ]
            )
            for idx, section in enumerate(sections)
        ]
    )

    payload_summary = {
        name: {
            "rows": len(frame.index),
            "columns": list(frame.columns),
        }
        for name, frame in datasets.items()
    }

    html = f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{config.report.get('title', 'Apocalypse Status Dashboard')}</title>
  <style>
    :root {{
      --bg: #f4f2eb;
      --fg: #16222a;
      --muted: #5d6a72;
      --accent: #26547c;
      --accent-2: #1f8a70;
      --panel: #ffffff;
      --border: #d9d5ca;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--fg);
      background: radial-gradient(circle at top left, #fffdf5 0%, var(--bg) 55%, #ece7da 100%);
      min-height: 100vh;
    }}
    header {{
      padding: 28px 24px 12px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(120deg, #f9f7ef 0%, #ece7da 100%);
    }}
    h1 {{ margin: 0; font-size: 2rem; letter-spacing: 0.01em; }}
    .subtitle {{ margin: 6px 0 0; color: var(--muted); }}
    .meta {{ margin-top: 10px; font-size: 0.9rem; color: var(--muted); }}

    .layout {{
      display: grid;
      grid-template-columns: 290px 1fr;
      gap: 0;
      min-height: calc(100vh - 120px);
    }}
    nav {{
      border-right: 1px solid var(--border);
      background: #fcfaf3;
      padding: 14px;
      overflow-y: auto;
    }}
    .tab-button {{
      width: 100%;
      text-align: left;
      border: 1px solid var(--border);
      background: var(--panel);
      padding: 10px 12px;
      margin: 0 0 8px;
      cursor: pointer;
      border-radius: 10px;
      color: var(--fg);
      transition: transform .12s ease, box-shadow .12s ease, background .12s ease;
      font-weight: 600;
    }}
    .tab-button:hover {{ transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,.08); }}
    .tab-button.active {{ background: var(--accent); color: white; border-color: var(--accent); }}

    main {{ padding: 20px; overflow: auto; }}
    .tab-panel {{ display: none; animation: fade .18s ease-in; }}
    .tab-panel.active {{ display: block; }}
    .tab-panel h2 {{ margin: 0 0 4px; }}
    .section-desc {{ margin: 0 0 14px; color: var(--muted); }}

    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .metric-card {{
      border: 1px solid var(--border);
      background: var(--panel);
      border-radius: 12px;
      padding: 12px;
    }}
    .metric-title {{ color: var(--muted); font-size: .9rem; }}
    .metric-value {{ font-size: 1.4rem; font-weight: 700; margin-top: 4px; color: var(--accent-2); }}

    .freshness-wrap {{
      margin: 4px 0 20px;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      background: var(--panel);
    }}
    .freshness-wrap h3 {{
      margin: 0 0 8px;
      font-size: 1rem;
      color: var(--fg);
    }}
    .freshness-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: .9rem;
    }}
    .freshness-table th, .freshness-table td {{
      border-top: 1px solid var(--border);
      padding: 7px 6px;
      text-align: left;
    }}
    .freshness-table thead th {{
      border-top: none;
      color: var(--muted);
      font-weight: 600;
    }}
    .data-source-table {{
      table-layout: fixed;
    }}
    .data-source-table .col-source {{ width: 14%; }}
    .data-source-table .col-dataset {{ width: 31%; }}
    .data-source-table .col-latest {{ width: 13%; }}
    .data-source-table .col-age {{ width: 10%; }}
    .data-source-table .col-links {{ width: 32%; }}
    .data-source-table td:nth-child(2) {{
      white-space: normal;
      overflow-wrap: anywhere;
    }}
    .data-source-table td:nth-child(3),
    .data-source-table th:nth-child(3),
    .data-source-table td:nth-child(4),
    .data-source-table th:nth-child(4) {{
      white-space: nowrap;
    }}
    .data-source-table td:nth-child(5) {{
      white-space: normal;
    }}
    .chart-wrap {{
      margin-bottom: 18px;
    }}
    .streamflow-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 10px;
    }}
    .streamflow-grid-card {{
      border: 1px solid var(--border);
      border-radius: 10px;
      background: var(--panel);
      padding: 6px;
    }}
    .chart-source {{
      margin: 8px 0 0;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fffdf8;
      padding: 10px 12px;
    }}
    .chart-source h4 {{
      margin: 0 0 6px;
      font-size: 0.95rem;
    }}
    .chart-source ul {{
      margin: 0;
      padding-left: 18px;
    }}
    .chart-source li {{
      color: var(--muted);
      line-height: 1.45;
      margin-bottom: 4px;
    }}
    .chart-source a {{
      color: var(--accent);
      word-break: break-all;
    }}

    .dataset-summary {{
      margin-top: 22px;
      background: #fff;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      font-size: .9rem;
      color: var(--muted);
    }}

    @keyframes fade {{
      from {{ opacity: 0; transform: translateY(2px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    @media (max-width: 960px) {{
      .layout {{ grid-template-columns: 1fr; }}
      nav {{ border-right: 0; border-bottom: 1px solid var(--border); max-height: 240px; }}
      .streamflow-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{config.report.get('title', 'Apocalypse Status Dashboard')}</h1>
    <p class=\"subtitle\">{config.report.get('subtitle', 'Offline cross-platform snapshot')}</p>
    <p class=\"meta\">Generated {generated}</p>
  </header>

  <div class=\"layout\">
    <nav>
      {nav_items}
    </nav>
    <main>
      {panels}
      <details class=\"dataset-summary\">
        <summary>Embedded Dataset Summary</summary>
        <pre>{json.dumps(payload_summary, indent=2)}</pre>
      </details>
    </main>
  </div>

  <script>
    const buttons = Array.from(document.querySelectorAll('.tab-button'));
    const panels = Array.from(document.querySelectorAll('.tab-panel'));

    function activate(target) {{
      buttons.forEach((button) => button.classList.toggle('active', button.dataset.target === target));
      panels.forEach((panel) => panel.classList.toggle('active', panel.id === target));
    }}

    buttons.forEach((button) => {{
      button.addEventListener('click', () => activate(button.dataset.target));
    }});
  </script>
</body>
</html>
"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path
