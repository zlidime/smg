"""
Run profiling.sql as one BigQuery script and dump every statement's result to a text file.

Setup (once):
  pip install google-cloud-bigquery db-dtypes pandas
  gcloud auth application-default login

Run:
  python run_profiling.py profiling.sql dime-smg-task > profiling_results.txt
"""
import sys
import pandas as pd
from google.cloud import bigquery

sql_file = sys.argv[1] if len(sys.argv) > 1 else "profiling.sql"
project = sys.argv[2] if len(sys.argv) > 2 else "dime-smg-task"

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 120)

client = bigquery.Client(project=project)
script = open(sql_file, encoding="utf-8").read()

parent = client.query(script)
parent.result()  # wait for whole script

# child jobs come back newest-first; reverse to script order
children = list(client.list_jobs(parent_job=parent.job_id))[::-1]

for i, job in enumerate(children, 1):
    stmt = (job.query or "").strip()
    first_line = next((ln for ln in stmt.splitlines() if ln.strip() and not ln.strip().startswith("--")), "")
    print("=" * 100)
    print(f"[{i}] {first_line[:160]}")
    print("-" * 100)
    if job.statement_type != "SELECT":
        print(f"({job.statement_type}) ok")
        continue
    try:
        df = job.to_dataframe()
        print(df.head(60).to_string(index=False) if len(df) else "(no rows)")
        if len(df) > 60:
            print(f"... {len(df)} rows total")
    except Exception as e:  # noqa: BLE001
        print(f"ERROR reading result: {e}")
    print()
