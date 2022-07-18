### 增加pool_info
#### 1、upload_csv
- upload_csv 执行前注意，stake_token / reward_token 是数组格式，在csv中需要用 - 分开
- 在pool_infos.csv增加平台数据后，执行upload_pools.py的upload_csv_pools方法即可

#### 2、sql
- 对应平台的pool_infos.sql放在平台文件夹下，参考 dex/ethereum/sushi/pool_infos.sql
- 字段参照以上或者dags/resources/stages/raw/schemas/pool_infos.json
- 放好之后执行upload_pools.py的run_dex_pool_info或者run_lending_pool_info方法
- 查询对应平台下是否生成pool_infos表 查询是否正常 (跟swap，liquidity，lending同一个数据集)

注意：
- 关于pool_infos的business_type字段，如果该pool只是swap类型的，填写trade；如果只是liquidity的，填写liquidity，需要填上lp_token字段；
如果该pool两者都是，填写dex；如果是lending类的，则填写lending
- 每个平台的sql需要加上时间筛选用来日增，参照sushi的pool_infos.sql，增加 {match_date_filter}

#### 3、生成视图
- 在all_project变量增加自己的平台
- 执行upload_pools.py的merge_pool_info_view方法
