from google.cloud import bigquery


def query_bigquery(query_string: str, project_id: str = 'footprint-etl-internal'):
    bqclient = bigquery.Client(project=project_id)
    dataframe = (
        bqclient.query(query_string)
            .result()
            .to_dataframe(create_bqstorage_client=False)
    )
    return dataframe
