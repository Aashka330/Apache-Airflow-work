# Airflow workflow automation

This project contains a dependency-free Airflow 3 workflow that runs a daily
sales reporting pipeline. The DAG extracts local records, aggregates revenue by
region, writes `output/daily_sales_report.json`, and validates the result.

## Run locally

Use the included Python environment and keep Airflow's metadata database inside
the project directory:

```sh
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
./airflow_venv/bin/airflow standalone
```

The standalone server prints login details and serves the Airflow UI at
`http://localhost:8080`. In another terminal, trigger the workflow:

```sh
export AIRFLOW_HOME="$PWD/.airflow"
./airflow_venv/bin/airflow dags test daily_sales_report 2026-09-06
```

The generated report is written to `output/daily_sales_report.json`. The DAG is
scheduled for 07:00 daily and does not backfill historical runs.

## Validate the DAG

```sh
export AIRFLOW_HOME="$PWD/.airflow"
AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags" ./airflow_venv/bin/airflow dags list --local
```
