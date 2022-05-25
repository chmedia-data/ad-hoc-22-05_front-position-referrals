import pandas as pd
import numpy as np
import os
from google.cloud import bigquery

os.chdir("/Users/adrianoesch/Documents/ad-hoc/22-05_front-position-referrals")

from bigquery import BigQuery
bq = BigQuery()

os.listdir("data")[0]

df = pd.read_parquet("data/"+os.listdir("data")[0])
df["time_15min"] = df.time.dt.floor("15min")

df.head()
df.section.value_counts()

job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("article_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("time_15min", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("position_on_list", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("container_rank", bigquery.enums.SqlTypeNames.INTEGER)
    ],
    # Optionally, set the write disposition. BigQuery appends loaded rows
    # to an existing table by default, but with WRITE_TRUNCATE write
    # disposition it replaces the table with the loaded data.
    write_disposition="WRITE_TRUNCATE",
)
df.shape
table_id = "chmedia.tmp_aaz_15min_front_placements"
job = bq.client.load_table_from_dataframe(
    df[["article_id","time_15min","position_on_list","container_rank"]],
    "chmedia.tmp_aaz_15min_front_placements",
    job_config=job_config
)
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
