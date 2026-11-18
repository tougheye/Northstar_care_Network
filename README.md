# Northstar Care Network — Provider Data Platform

A Databricks-based data platform that builds an analytical view of healthcare providers for the Northstar Care Network: drug-manufacturer payment transparency data (Open Payments-style), CMS Medicare provider utilization and comparison datasets, the NPPES NPI registry, excluded-individual (LEIE-style) lists, and ZCTA 2020 census geography.

The project follows a **medallion architecture** (bronze → silver → gold) on Unity Catalog, built with serverless **Lakeflow Spark Declarative Pipelines (SDP)**, Auto Loader ingestion from UC volumes, the data.cms.gov REST API, and a **Declarative Automation Bundle (DAB)** for deployment.

## Architecture at a Glance

```
UC Volumes + data.cms.gov API          Silver (typed, cleansed)          Gold (conformed analytics)
┌──────────────────────────┐   ┌──────────────────────────────┐   ┌──────────────────────────────┐
│ Bronze (raw as landed)    │ → │ northstar_catalog_silver      │ → │ northstar_catalog_gold        │
│ northstar_network_bronze  │   │ payment_data_silver          │   │ dim_provider                  │
│ (Auto Loader / append)   │   │ exclude_indiv_silver         │   │ fact_provider_payments        │
└──────────────────────────┘   │ cms_prov_comp_silver          │   │ provider_exclusion_flags      │
                                │ cms_provider_api_silver       │   │ provider_network_summary      │
                                └──────────────────────────────┘   └──────────────────────────────┘
```

All pipelines run on **serverless compute with Photon** in `CURRENT` channel mode.

## Catalogs

| Catalog | Purpose |
| --- | --- |
| `northstar_network_bronze` | Raw, as-landed bronze data (all pipeline event logs land in `pipeline_data_bronze`) |
| `northstar_catalog_silver` | Cleansed, typed, deduplicated silver tables |
| `northstar_catalog_gold` | Planned gold layer — see the Gold Layer section below |

## Data Sources & Bronze Ingestion

| Source | Landing volume | Bronze table | Notes |
| --- | --- | --- | --- |
| NSN payment data (Open Payments-style CSV) | `/Volumes/northstar_network_bronze/payment_data_bronze/nsn_payment_data` | `northstar_network_bronze.payment_data_bronze.payment_data_bronze` | ~3.5M rows; partitioned by `Covered_Recipient_Specialty_1`; profile/ NPI cast to bigint |
| Excluded individuals list | `/Volumes/northstar_network_bronze/excluded_indiv_bronze/excluded_individual_raw_data` | `northstar_network_bronze.excluded_indiv_bronze.excluded_indiv_data_table` | ~84K rows; partitioned by `STATE` |
| CMS provider comparison | `/Volumes/northstar_network_bronze/cms_provider_compare/cms_provider_compare_storage` | `northstar_network_bronze.cms_provider_compare.cms_prov_comp_ingest_data` | Column names sanitized (spaces → underscores); partitioned by `pri_spec`; `delta.columnMapping.mode = name` |
| CMS API — Medicare Physician & Other Practitioners by Provider and Service | data.cms.gov REST API | `northstar_network_bronze.cms_provider_specialty.api_ingestion_table` | Widget-driven, specialty-by-specialty append; see below |
| NPPES NPI registry | `/Volumes/northstar_network_bronze/nppes_data` | `northstar_network_bronze.nppes_data.npi_bronze` | Header CSV supplies schema (331 string columns); streaming with checkpoint in the volume |
| ZCTA 2020 census geography | `/Volumes/northstar_network_bronze/zcta_2020_dta/zcta_2020` | `northstar_network_bronze.zcta_2020_dta.zcta_2020_bronze` | Pipe-delimited, created via SQL `read_files()` (see `zip_data_ingestion.py`) |

Every Auto Loader bronze table adds `ingestion_date`, `ingestion_timestamp`, `source_file_path`, and `source_file_name` audit columns, and uses `cloudFiles.schemaEvolutionMode = rescue` so schema drift is quarantined rather than lost.

### CMS API ingestion (`CMS_provider_API_calls/`)

* `api_func.py` — `fetch_cms()` pages the data.cms.gov API (dataset `92396110-…`, 5,000 rows/page) filtered by provider type.
* `cms_prov_api_ingest.py` — notebook with a `specialty` widget. It checks the bronze table first and only pulls a specialty that is not already present, then appends with `_ingest_date` / `_ingest_time` audit columns. (Last source refresh covered data through calendar year 2024.)

## Pipelines

### `NCN_Payment_Ingestion_Pipeline` (main SDP pipeline)

Runs over `NCN_Ingestion_Pipelines/transformations/**` with these configuration keys:

| Key | Value |
| --- | --- |
| `payment_data_volume` | `/Volumes/northstar_network_bronze/payment_data_bronze` |
| `excluded_indiv_data_volue` | `/Volumes/northstar_network_bronze/excluded_indiv_bronze/excluded_individual_raw_data` |
| `cms_prov_comp_volume_path` | `/Volumes/northstar_network_bronze/cms_provider_compare/cms_provider_compare_storage` |

Pipeline event log: `northstar_network_bronze.pipeline_data_bronze.event_log_bronze_ingestion`.

### `cms_provider_api_silver`

Separate SDP pipeline for the SCD Type 2 silver table built off the CMS API bronze data (source code currently lives in `Silver_pipelines/provider_api_silver.py`).

## Silver Layer (`northstar_catalog_silver`)

| Table | Type | Transformation |
| --- | --- | --- |
| `payment_data_silver.payment_data_silver` | Streaming table | Renames Open Payments-style columns to a doctor-centric model, casts amounts/dates, enforces expectations (`valid_payment`, `Has_specialty`, `Has_NPI` — invalid rows dropped), enriches each payment with the county name via a left join to ZCTA 2020 on `Bus_Zip` |
| `payment_data_silver.payment_data_quarantine` | Streaming table | The expectation rejects (null payment amount, specialty, or NPI) kept for review instead of being silently dropped |
| `exclude_indiv_silver.exclude_indiv_silver` | Streaming table (Auto CDC, SCD1) | Keyed on (`NPI`, `DOB`, `EXCLDATE`), sequenced by `ingestion_timestamp`; rows with clerical/sales-type specialties are applied as deletes, keeping the list to real clinical exclusions |
| `cms_prov_comp_silver.prov_comp_mv` | Materialized view | Deduplicates the provider-compare feed per (`NPI`, `Ind_PAC_ID`, `Ind_enrl_ID`), summing `num_org_mem` and using `max_by(..., ingestion_timestamp)` survivorship for demographics, credentials, and facility address |
| `cms_provider_api_silver.silver_providers` | Streaming table (Auto CDC, SCD2) | Typing (`try_cast` to DOUBLE), positive-amount expectations, keyed on (`Rndrng_NPI`, `HCPCS_Cd`, `Place_Of_Srvc`) sequenced by `_ingest_time` — full change history retained via `__START_AT` / `__END_AT` |

### Data quality & testing

* Expectation-driven quarantine on payments (silver vs. quarantine tables).
* `NSN_pipeline_data_explore.sql` — exploratory notebook used to profile bronze/silver (payment ranges, null counts, exclusion specialties) and contains a unit test comparing distinct specialty counts between the excluded-individual bronze and silver tables.

## Gold Layer (`northstar_catalog_gold`) — Planned

> **Note:** the gold layer is designed but not yet materialized. The tables below are the target design for the analytics/consumer layer; none of them exist yet in the catalog.

The gold layer will conform the silver tables into provider-centric analytics:

| Table (planned) | Contents |
| --- | --- |
| `dim_provider` | Conformed provider dimension: one row per `doctor_NPI`, coalescing NPPES identity, provider-compare demographics/credentials (from `prov_comp_mv`), and the SCD2 provider history in `silver_providers` |
| `fact_provider_payments` | One row per payment (`Record_ID`), typed and enriched (county, product category, payer), with a surrogate key into `dim_provider` |
| `provider_exclusion_flags` | Providers in `fact_provider_payments` joined to `exclude_indiv_silver`, flagging currently excluded individuals and their exclusion dates |
| `provider_network_summary` | Aggregates: total and average payments by specialty, county, payer, and program year, with exclusion-adjusted totals for network analysis |

Intended consumers: provider-network dashboards and ad-hoc SQL on top of `provider_network_summary` / `fact_provider_payments`.

## Repository Layout

```
Northstar_care_Network/
├── NCN_Ingestion_Pipelines/transformations/    # Main SDP pipeline source (bronze + silver)
│   ├── payment_ingestion.py                    # Payment bronze (Auto Loader)
│   ├── excl_and_cms_care_ingest.py             # Excluded-individual + provider-compare bronze
│   ├── payment_silver_pipeline.py              # Payment silver + quarantine (expectations, ZCTA county join)
│   ├── excluded_Indiv_silver_trans.py          # Excluded-individual silver (Auto CDC SCD1)
│   └── cms_prov_comp_silver_mv.py              # Provider-compare silver MV (dedup + survivorship)
├── Silver_pipelines/
│   └── provider_api_silver.py                 # CMS API silver (Auto CDC SCD2) — runs in cms_provider_api_silver pipeline
├── CMS_provider_API_calls/
│   ├── api_func.py                             # data.cms.gov API paging function
│   └── cms_prov_api_ingest.py                  # Specialty widget-driven API ingestion notebook
├── NPI_ingestion_streaming_table.py           # NPPES bronze streaming ingestion notebook
├── NSN_pipeline_data_explore.sql               # Data profiling / unit-test notebook
├── zip_data_ingestion.py                      # ZCTA 2020 bronze creation query (pipe-delimited read_files)
└── Northstar_DAB/                             # Declarative Automation Bundle (dev + prod targets)
```

## Deployment (Declarative Automation Bundle)

The `Northstar_DAB` bundle has two targets, both on `https://dbc-38c471d1-00a8.cloud.databricks.com`:

* **dev** (default) — development mode.
* **prod** — production mode, resources under `/Users/mkamin.6696@gmail.com/.bundle/Northstar_DAB/prod`, run as `mkamin.6696@gmail.com`.

A separate local (VS Code) bundle, `Northstar_project`, additionally deploys a development copy of the pipeline as `[dev mkamin_6696] NCN_Payment_Ingestion_Pipeline Bundle`.

Deploy via the workspace **Deployments** panel, or see the [DAB docs](https://docs.databricks.com/dev-tools/bundles/workspace-bundles).

## Operational Notes

* Bronze streaming tables were originally created with `delta.appendOnly = true`; that property has since been relaxed (`delta.appendOnly = false`, `pipelines.reset.allowed = true`) so full refreshes work. If a legacy table still rejects a full refresh, run a one-time `ALTER TABLE <table> SET TBLPROPERTIES (delta.appendOnly=false)`.
* NPPES bronze column names contain dots/parentheses (e.g. `…Country_Code_If_outside_U.S.`) — always quote them with backticks or alias on first use.
* Pipelines run triggered (not continuous); Auto Loader picks up new files in the volumes on each update.