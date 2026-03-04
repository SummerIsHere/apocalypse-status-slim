# Apocalypse Status Dashboard (Modernized)

## Purpose
This repository downloads climate and resource data, builds tidy and modeled datasets, and renders a single offline HTML dashboard that works on both macOS and Windows.

The Power BI `.pbix` artifacts are kept for historical reference, but they are no longer required for normal use.

## What Changed
- New Python package layout under `src/apocalypse_status`.
- New CLI: `apoc`.
- Raw and tidy outputs are split:
  - `downloaded_data/raw/...`
  - `downloaded_data/tidy/...`
- Offline dashboard artifact:
  - `artifacts/apocalypse_status_dashboard.html`
- Run metadata and quality report:
  - `artifacts/run_manifest.json`
- Streamflow now supports provisional values by default and flags status at monthly level.

## Measures Tracked
The dashboard keeps the original wiki intent, with a modernized pipeline:

- Climate Change (global):
  - Atmospheric CO2 concentration (Mauna Loa, monthly).
  - Global near-surface temperature anomaly (land + sea).
  - Historical CO2 context from legacy ice-core composites (when available in repo data).
- Climate Change (Puget Sound / WA focus):
  - Annual average air temperature.
  - Heating and cooling degree days.
  - Seasonal precipitation.
  - Snowpack (SWE) trends.
  - River streamflow by month.
  - Mean sea-level change.
- Resource Depletion:
  - Grain per-capita trend (global supply relative to population).

## Threshold Context
The HTML charts include practical reference lines aligned with the prior wiki framing:

- CO2:
  - `350 ppm`: commonly cited "safe line" context from James Hansen-style guidance.
  - `415 ppm`: approximate Pliocene-era context threshold used in the historical wiki narrative.
- Global temperature anomaly:
  - Dashboard chart lines currently highlight `+1.5 C` and `+3.0 C` for near-term interpretability.
  - The old wiki also documented higher warming scenario context (`~4 C`, Eocene-era analog references).

## Data Source Lineage (Wiki + Current Pipeline)
Primary sources in the current code map to the prior wiki source list:

- NOAA GML Mauna Loa CO2 (wiki `E`): [https://gml.noaa.gov/ccgg/trends/](https://gml.noaa.gov/ccgg/trends/)
- Met Office HadCRUT (wiki `F`): [https://www.metoffice.gov.uk/hadobs/hadcrut5/](https://www.metoffice.gov.uk/hadobs/hadcrut5/)
- EPA legacy CO2 composite context (wiki `H`): [https://www.epa.gov/sites/default/files/2016-08/ghg-concentrations_fig-1.csv](https://www.epa.gov/sites/default/files/2016-08/ghg-concentrations_fig-1.csv)
- NOAA nClimDiv (wiki `I`): [https://www.ncei.noaa.gov/pub/data/cirs/climdiv/](https://www.ncei.noaa.gov/pub/data/cirs/climdiv/)
- USDA NRCS SNOTEL (wiki `J`): [https://wcc.sc.egov.usda.gov/reportGenerator/](https://wcc.sc.egov.usda.gov/reportGenerator/)
- USGS NWIS streamflow (wiki `K`): [https://waterdata.usgs.gov/nwis](https://waterdata.usgs.gov/nwis)
- NOAA CO-OPS tide/sea level (wiki `L`): [https://tidesandcurrents.noaa.gov/](https://tidesandcurrents.noaa.gov/)
- USDA PSD grain data (wiki `G`): [https://apps.fas.usda.gov/psdonline/app/index.html#/app/home](https://apps.fas.usda.gov/psdonline/app/index.html#/app/home)
- World Bank population API (wiki `C` lineage): [https://data.worldbank.org/indicator/SP.POP.TOTL](https://data.worldbank.org/indicator/SP.POP.TOTL)
- US Census IDB population API (newer complement used by this pipeline): [https://api.census.gov/data/timeseries/idb/5year](https://api.census.gov/data/timeseries/idb/5year)

## Installation
1. Use Python 3.11+.
2. Install dependencies:

```bash
python -m pip install -e .
```

3. Optional dev tooling:

```bash
python -m pip install -e .[dev]
pre-commit install
```

## Quick Start
Run the entire pipeline:

```bash
apoc run all
```

This fetches sources, transforms data, validates outputs, and creates:
- `artifacts/apocalypse_status_dashboard.html`
- `artifacts/run_manifest.json`

Open the HTML file in any browser on macOS or Windows.

## CLI Commands
- Fetch raw data:

```bash
apoc fetch
```

- Force a refresh even if today's fetch already succeeded:

```bash
apoc fetch --force
```

- Transform datasets:

```bash
apoc transform
```

- Validate datasets:

```bash
apoc validate
```

- Build HTML dashboard:

```bash
apoc report html
```

- Capture baseline metrics from currently tracked CSV files:

```bash
apoc baseline
```

## Streamflow Provisional Data
Default behavior includes approved + provisional streamflow values and computes monthly status fields:
- `data_status` (`approved`, `provisional`, `mixed`)
- `provisional_day_count`
- `approved_day_count`
- `month_completeness`
- `is_provisional_month`

To force approved-only behavior:

```bash
apoc run all --streamflow-approved-only
```

## Daily Fetch Skipping
- Successful source fetches are cached per day in `artifacts/fetch_state.json`.
- If a source already succeeded today and its raw files still exist, `apoc fetch`/`apoc run all` skips re-fetching that source.
- SNOTEL also tracks per-station daily success, so if a partial SNOTEL run occurs, successful stations are skipped while failed stations are retried.
- SNOTEL mirror support:
  - successful network fetches write mirror snapshots under `downloaded_data/raw/snotel_mirror`
  - optional mirror-first mode serves from fresh mirror files before hitting the network
  - automatic mirror fallback is used when network fetch fails and a mirror exists
- Use `--force` to bypass skipping and fetch again.

## SNOTEL Station List Validation
Use the AWDB-backed validator to rebuild or check the station list against authoritative metadata:

```bash
python scripts/validate_snotel_station_list.py --check
python scripts/validate_snotel_station_list.py --write
```

The validator normalizes to one row per station with canonical fields such as:
- `station_triplet`
- `snotel_station_id`
- `state_cd`
- `network_cd`
- `station_name`
- `snow_survey_program_station_code`
- `latitude`, `longitude`, `elevation_ft`
- `county_name`, `huc`, `begin_date`, `end_date`, `shef_id`
- `legacy_regions`, `wria_id`, `wria_name`
- `region_group` (`Puget Sound` or `Other WA`, derived WRIA-first with legacy fallback)

WRIA enrichment is incremental by default:
- Existing `wria_id`/`wria_name` values are reused when station coordinates are unchanged.
- New or coordinate-changed stations are looked up via the Washington WRIA polygon service.
- Use `--refresh-wria` to force a full WRIA reclassification pass.

## Legacy Compatibility
- `main.py` now acts as a compatibility shim and calls the modern pipeline.
- Legacy compatibility CSVs are still written into existing `downloaded_data/...` paths.

## Notes
- Full datasets remain tracked in git by project choice.
- If network sources change schema or endpoint behavior, update `config/sources.yaml` and rerun `apoc validate`.
