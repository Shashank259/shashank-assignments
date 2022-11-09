#Python Module to interact with BigQuery data
from google.cloud import bigquery





class BigQueryUtils:

    def bq_table_to_df(self,project, table_name ):#funtion to return a df after reading BQ table
        client = bigquery.Client(project = f"{project}")
        job_config = bigquery.QueryJobConfig()
        sql=f"select * from `{table_name}`;"
        query_job = client.query(sql, job_config=job_config)
        df = query_job.result().to_dataframe()
        return df
        

    def update_bq_table(self,df, project, table_name ):#funtion to update(overwrite) BQ table
        client = bigquery.Client(project = f"{project}")
        job_config = bigquery.QueryJobConfig()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_name, job_config=job_config)  
        job.result()
        return
        

    def insert_df_bq_table(self,df, project, table_name ):#funtion to append(add df into table) BQ table
        client = bigquery.Client(project = f"{project}")
        job_config = bigquery.QueryJobConfig()
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_name, job_config=job_config)  
        job.result()
        return