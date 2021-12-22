def client_query():
    from google.cloud import bigquery
    import time
    import sys
    client = bigquery.Client()

    table_id = "freud-int-200423.neogames.function-v1"
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = 'WRITE_APPEND'
    
    query = """
        SELECT x,y
        FROM freud-int-200423.iot_table.iot_data_geo
        WHERE 1=1
        """
    query_job = client.query(query, job_config=job_config)
    query_job.result()
client_query()
