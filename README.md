# Healthcare Data Warehouse

A modern data warehouse built on PostgreSQL using CMS Medicare synthetic claims data (DE-SynPUF) and supplementary Kaggle encounter data. The project follows a medallion architecture (bronze → silver → gold) and is designed to support healthcare analytics use cases such as utilization, cost, and chronic condition analysis.

---

## Architecture

This project follows a three-layer medallion architecture:

| Layer | Schema | Description |
|---|---|---|
| Bronze | `staging` | Raw data landed as-is from source CSVs. All columns typed as `TEXT`. No transformations. |
| Silver | *(in progress)* | Cleaned, typed, and deduplicated data. Dates cast, numerics converted, nulls handled. |
| Gold | *(in progress)* | Dimensional model (facts + dimensions) optimized for analytics and reporting. |

---

## Data Sources

### CMS 2008–2010 DE-SynPUF
Synthetic Medicare claims data published by the Centers for Medicare & Medicaid Services. Based on a 5% sample of 2008 Medicare beneficiaries with claims from 2008–2010. Contains ~2.3M synthetic beneficiaries across 20 subsamples.

| File Type | Staging Table | Columns |
|---|---|---|
| Beneficiary Summary (2008) | `staging.beneficiary_2008` | 32 |
| Beneficiary Summary (2009) | `staging.beneficiary_2009` | 32 |
| Beneficiary Summary (2010) | `staging.beneficiary_2010` | 32 |
| Inpatient Claims | `staging.inpatient_claims` | 81 |
| Outpatient Claims | `staging.outpatient_claims` | 76 |
| Carrier Claims (Physician) | `staging.carrier_claims` | 142 |
| Prescription Drug Events | `staging.prescription_drug_events` | 8 |

> Note: Carrier claims are released as two CSV files per sample (segment A and B) but are loaded into a single table.

### Kaggle Hospital Encounters
A synthetic hospital encounter dataset used to supplement the CMS claims data with additional encounter-level attributes.

| File Type | Staging Table |
|---|---|
| Hospital Encounters | `staging.kaggle_encounters` |

---

## Tech Stack

- **Database:** PostgreSQL (local)
- **Orchestration:** *(planned)*
- **Transformation:** *(planned)*
- **Visualization:** *(planned)*

---

## Project Structure

```
healthcare-data-warehouse/
└── sql/
    ├── 01_create_staging.sql    # Bronze layer: creates all staging tables
    └── ...
```

---

## Setup

### Prerequisites
- PostgreSQL installed (via Homebrew recommended on macOS)
- Source CSV files downloaded from [CMS DE-SynPUF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files)

##### 1. Confirm PostgreSQL is running

```bash
brew services list
```

If stopped, start it with:

```bash
brew services start postgresql@<version>
```

##### 2. Create the database

```bash
psql -d postgres -c "CREATE DATABASE healthcare_dw;"
```

##### 3. Create the bronze layer

```bash
psql -d healthcare_dw -f sql/01_create_staging.sql
```

##### 4. Load the bronze layer

```bash
psql -d healthcare_dw -f sql/02_load_staging.sql
```

#####  5. Verify

```bash
psql -d healthcare_dw
```

Then inside psql:

```sql
\dt staging.*
```

You should see all staging tables listed.

---

## Silver Layer Strategy

The silver layer focuses on **data quality, type conversion, and deduplication**. It transforms raw staging data into a clean, validated foundation for the gold layer.

### Transformation Steps

1. **Create silver schema** — Establish `silver` schema to mirror `staging` tables

2. **Data type conversion** — Cast TEXT columns to appropriate types (DATE, NUMERIC, INTEGER, BOOLEAN); handle invalid/malformed values gracefully

3. **Null & missing value handling** — Document null patterns per table; apply business logic (e.g., missing claim amounts → 0, invalid dates → NULL)

4. **Deduplication** — Identify natural keys for each table (e.g., beneficiary_id + claim_id); remove exact duplicates and keep most recent record

5. **Standardization of coded values** - The SynPUF data contains numeric codes (e.g. BENE_SEX_IDENT_CD is 1/2, BENE_RACE_CD is 1-6, chronic condition flags are 1/2 instead of true/false). Decoding those to human-readable values so that gold layer is cleaner. 

6. **Data validation** — Add NOT NULL constraints where appropriate; add CHECK constraints for logical rules (e.g., end_date ≥ start_date); add FOREIGN KEY relationships

7. **Additive columns** — Add `_loaded_at` timestamp (batch load time); add `_row_hash` for change detection (useful later)

8. **Indexing** — Create indexes on foreign keys and commonly filtered columns

---

## References

- [CMS DE-SynPUF User Manual](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/SynPUF_DUG.pdf)
- [Building a Modern Data Warehouse from Scratch](https://rihab-feki.medium.com/building-a-modern-data-warehouse-from-scratch-d18d346a7118)