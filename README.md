# Dorado

Dorado is a daily batch pipeline that computes the four DORA metrics for a handful of well-known open-source repositories. It reads the public [GH Archive](https://www.gharchive.org/) event stream, aggregates it with PySpark, checks the result, and loads it into BigQuery. Airflow schedules and backfills the runs.

The DORA metrics measure how a team delivers software:

| Metric | Question it answers |
| --- | --- |
| Deployment frequency | How often does the project ship? |
| Lead time for changes | How long does a change take from proposal to merge? |
| Change failure rate | How often does a change need to be undone or fixed? |
| Time to restore | How long does it take to resolve a reported bug? |

## Architecture

```mermaid
flowchart LR
    gha[("GH Archive<br/>hourly .json.gz")]

    subgraph local["Docker Compose"]
        direction LR
        subgraph airflow["Airflow DAG (daily)"]
            direction LR
            download["Download<br/>24 hourly files"]
            spark["Spark job<br/>filter and aggregate"]
            checks["Data-quality<br/>checks"]
            load["Load<br/>partition"]
        end
        raw[("Raw files")]
        metrics[("Daily metrics<br/>Parquet")]
    end

    subgraph gcp["Google Cloud"]
        table[("BigQuery table<br/>partitioned by date")]
        views["SQL views<br/>one per metric"]
    end

    gha --> download --> raw --> spark --> metrics
    metrics --> checks --> load --> table --> views
```

All heavy lifting happens locally. Spark reads a full day of raw events, keeps only the configured repositories, and writes a few hundred rows of daily aggregates. Only those aggregates travel to BigQuery. The pipeline never queries the public `githubarchive` dataset in BigQuery, so it stays within the free sandbox.

Each daily run is one Airflow task chain:

```mermaid
flowchart LR
    a[download_hours] --> b[run_spark_job] --> c[check_quality] --> d[load_to_bigquery]
    c -. on failure .-> n[notify]
```

A failed check stops the run before anything reaches BigQuery. Each load replaces exactly one date partition, so rerunning or backfilling a day never duplicates data.

