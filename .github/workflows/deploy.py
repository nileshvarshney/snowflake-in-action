import os
import snowflake.connector
import glob

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")
)

cs = conn.cursor()
try:
    sql_files = sorted(glob.glob("sql/*.sql"))
    for file in sql_files:
        with open(file, "r") as f:
            sql = f.read()
            print(f"Executing {file}...")
            cs.execute(sql)
            print(f"Executed {file} successfully.")
finally:
    cs.close()
    conn.close()