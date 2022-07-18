# ETL Airflow

使用 Airflow DAGs 来从各个数据源获取数据并上传到 bigquery 中

- [uniswap_etl_dag.py](dags/temp/uniswap_etl_dag.py) - exports uniswap pair data to a table.

## Prerequisites 前置要求

- linux/macos terminal
- git
- [gcloud](https://cloud.google.com/sdk/install)

## Setting Up 初始化

1. Create a GCS bucket to hold export files 创建 bucket 存储临时数据:

   ```bash
   gcloud config set project <your_gcp_project>
   PROJECT=$(gcloud config get-value project 2> /dev/null)
   ENVIRONMENT_INDEX=0
   BUCKET=${PROJECT}-${ENVIRONMENT_INDEX}
   gsutil mb gs://${BUCKET}/
   ```
   
2. Create a Google Cloud Composer environment 创建 Airflow 容器:

   ```bash
   ENVIRONMENT_NAME=${PROJECT}-${ENVIRONMENT_INDEX} && echo "Environment name is ${ENVIRONMENT_NAME}"
   gcloud composer environments create ${ENVIRONMENT_NAME} --location=us-central1 --zone=us-central1-a \
       --disk-size=30GB --machine-type=n1-standard-1 --node-count=3 --python-version=3 --image-version=composer-1.10.6-airflow-1.10.3 \
       --network=default --subnetwork=default

   gcloud composer environments update $ENVIRONMENT_NAME --location=us-central1 --update-pypi-package=polygon-etl==0.0.17
   ```

   Note that if Composer API is not enabled the command above will auto prompt to enable it.

3. This will be a good time to go to the bigquery console and cretae new datasets named "footprint_etl" under your project.
   
   确保在 bigquery 已经存在 footprint_etl 这个数据集，没有则新建一个
   
4. 根据下文的 [Configuring Airflow Variables](#configuring-airflow-variables) 指引配置必要变量
5. 根据下文的 [Deploying Airflow DAGs](#deploying-airflow-dags) 部署 DAGs 到 Cloud Composer Environment.
6. 根据下文的 [here](https://cloud.google.com/composer/docs/how-to/managing/creating#notification)
   配置邮件提醒.


## Configuring Airflow Variables 配置 Airflow 变量
- For a new environment clone polygon ETL Airflow: `git clone https://github.com/blockchain-etl/polygon-etl && cd polygon-etl/airflow`.
  For an existing environment use the `airflow_variables.json` file from
  [Cloud Source Repository](#creating-a-cloud-source-repository-for-airflow-variables) for your environment.
- Copy `example_airflow_variables.json` to `airflow_variables.json`.
  Edit `airflow_variables.json` and update configuration options with your values.
  You can find variables description in the table below. For the `polygon_output_bucket` variable
  specify the bucket created on step 1 above. You can get it by running `echo $BUCKET`.
- Open Airflow UI. You can get its URL from `airflowUri` configuration option:
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Navigate to **Admin > Variables** in the Airflow UI, click **Choose File**, select `airflow_variables.json`,
  and click **Import Variables**.
  

### Airflow Variables

Note that the variable names must be prefixed with `{chain}_`, e.g. `polygon_output_bucket`.

| Variable                         | Description                                                                                                                                                     |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `output_bucket`                  | GCS bucket where exported files with blockchain data will be stored                                                                                             |
| `export_start_date`              | export start date, default: `2019-04-22`                                                                                                                        |
| `export_end_date`                | export end date, used for integration testing, default: None                                                                                                    |
| `export_schedule_interval`       | export cron schedule, default: `0 1 * * *`                                                                                                                      |
| `provider_uris`                  | comma-separated list of provider URIs for [polygon-etl](https://polygon-etl.readthedocs.io/en/latest/commands) command                                          |
| `notification_emails`            | comma-separated list of emails where notifications on DAG failures, retries and successes will be delivered. This variable must not be prefixed with `{chain}_` |
| `export_max_active_runs`         | max active DAG runs for export, default: `3`                                                                                                                    |
| `export_max_workers`             | max workers for [polygon-etl](https://polygon-etl.readthedocs.io/en/latest/commands) command, default: `5`                                                      |
| `destination_dataset_project_id` | GCS project id where destination BigQuery dataset is                                                                                                            |
| `load_schedule_interval`         | load cron schedule, default: `0 2 * * *`                                                                                                                        |
| `load_end_date`                  | load end date, used for integration testing, default: None                                                                                                      |

## Deploying Airflow DAGs 部署 DAGs

- Get the value from `dagGcsPrefix` configuration option from the output of:
  `gcloud composer environments describe ${ENVIRONMENT_NAME} --location us-central1`.
- Upload DAGs to the bucket. Make sure to replace `<dag_gcs_prefix>` with the value from the previous step:
  `./upload_dags.sh <dag_gcs_prefix>`.
- To understand more about how the Airflow DAGs are structured
  read [this article](https://cloud.google.com/blog/products/data-analytics/ethereum-bigquery-how-we-built-dataset).
- Note that it will take one or more days for `polygon_export_dag` to finish exporting the historical data.
- To setup automated deployment of DAGs refer to [Cloud Build Configuration](/docs/cloudbuild-configuration.md).

装包
`gcloud composer environments update $ENVIRONMENT_NAME --update-pypi-package=google-cloud-bigquery`
`gcloud composer environments update ethereum-etl-0 --location us-central1 --update-pypi-package=gql==3.0.0a6`

## Integration Testing

It is [recommended](https://cloud.google.com/composer/docs/how-to/using/testing-dags#faqs_for_testing_workflows) to use a dedicated Cloud Composer
environment for integration testing with Airflow.

To run integration tests:

- Create a new environment following the steps in the [Setting Up](#setting-up) section.
- On the [Configuring Airflow Variables](#configuring-airflow-variables) step specify the following additional configuration variables:
  - `export_end_date`: `2020-05-30`
  - `load_end_date`: `2020-05-30`
- This will run the DAGs only for the first day. At the end of the load DAG the verification tasks will ensure
  the correctness of the result.