from defi360.common.bigquery_adapter import BigqueryAdapter

class BusNewAddressUpload(BigqueryAdapter):

    project_id = 'gaia-data'
    data_set = "gaia"
    task_name = "ud_business_type_new_address"
    # time_partitioning_field = "first_day"
    execution_time = "1 5 * * *"
    history_date = "2022-03-29"
    schema_name = "defi360/schema/business_type_new_address.json"
    sql_path = "defi360/bigquery_adapter/NewAddress/ud_business_type_new_address.sql"

if __name__ == '__main__':
    busNewAddressUpload = BusNewAddressUpload()
    # busNewAddressUpload.load_history_data()

    busNewAddressUpload.load_daily_data(debug=False)

    # busNewAddressUpload.create_data_view()

    print('upload done')