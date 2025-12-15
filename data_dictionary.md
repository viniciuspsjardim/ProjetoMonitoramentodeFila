# Data Dictionary & Schema Definitions (Prompt 03)

## Definitions
- **Wait Time**:
    - `wait_triage`: `triage_start` - `arrival_time`
    - `wait_service`: `care_start` - `triage_end` (or `arrival_time` if triage skipped, logic defined in Silver)
    - `total_wait`: `care_start` - `arrival_time`
- **Occupancy**: `occupied_beds` / `functional_capacity`

## 1. Bronze Layer (Raw Archive)
**Format**: Parquet
**Partitioning**: `dt=YYYY-MM-DD`

### `encounters_raw`
| Field | Type | Description |
|---|---|---|
| encounter_id | string | UUID |
| arrival_time | timestamp | UTC |
| patient_id | string | Pseudonymized |
| sector_id | string | e.g., 'Emergency', 'Pediatrics' |
| triage_level | int | 1-5 (Manchester) |
| dt | date | Partition Key |

### `events_raw`
| Field | Type | Description |
|---|---|---|
| event_id | string | UUID |
| encounter_id | string | FK |
| event_type | string | 'triage_start', 'triage_end', 'care_start', 'discharge' |
| event_timestamp | timestamp | UTC |
| dt | date | Partition Key |

## 2. Silver Layer (Cleaned & Enriched)
**Format**: Parquet
**Partitioning**: `dt=YYYY-MM-DD` / `sector_id`

### `encounters`
| Field | Type | Description |
|---|---|---|
| encounter_id | string | PK |
| arrival_time | timestamp | |
| triage_start | timestamp | |
| triage_end | timestamp | |
| care_start | timestamp | |
| discharge_time | timestamp | |
| wait_triage_min | float | |
| wait_service_min | float | |
| los_min | float | Length of Stay |

## 3. Gold Layer (Aggregated & ML)
**Format**: Parquet
**Partitioning**: `dt=YYYY-MM-DD`

### `ops_metrics_hourly` (Aggregated)
| Field | Type | Description |
|---|---|---|
| dt | date | Partition Key |
| hour | int | 0-23 |
| sector_id | string | |
| arrivals_count | int | |
| wait_avg_60m | float | |
| occupancy_rate | float | |
| risk_score | float | 0-100 (Overload risk) |

### `predictions` (ML Output)
| Field | Type | Description |
|---|---|---|
| prediction_timestamp | timestamp | When prediction was made |
| horizon | string | '+1h', '+4h', '+24h' |
| predicted_occupancy | float | |
| predicted_wait_time | float | |
| model_version | string | |
