from defi360.common.bigquery_adapter import BigqueryAdapter

class NewAddressUpload(BigqueryAdapter):

    project_id = "gaia-data"
    data_set = "gaia"
    task_name = "ud_protocol_new_address"
    # time_partitioning_field = "first_day"
    execution_time = "1 5 * * *"
    history_date = "2022-03-29"
    schema_name = "defi360/schema/protocol_new_address.json"
    sql_path = "defi360/bigquery_adapter/NewAddress/ud_protocol_new_address.sql"

if __name__ == '__main__':
    newAddressUpload = NewAddressUpload()

    # newAddressUpload.load_history_data()

    newAddressUpload.load_daily_data(debug=False)

    # newAddressUpload.create_data_view()

    print('upload done')