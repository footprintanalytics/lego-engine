from bridges.dune.eth_bridges_tvl_over_time import EthBridgesTvlOverTime
from bridges.dune.eth_bridges_asset_distribution import EthBridgesAssetDistribution
from bridges.dune.eth_bridge_daily_unique_depositors import EthBridgesDailyUniqueDepositors

class BridgesScrapyAll(object):
    eth_bridges_tvl_over_time = EthBridgesTvlOverTime()
    eth_bridges_asset_distribution = EthBridgesAssetDistribution()
    eth_bridges_daily_unique_depositors = EthBridgesDailyUniqueDepositors()

    def exec(self):
        self.eth_bridges_daily_unique_depositors.save_data()
        self.eth_bridges_daily_unique_depositors.handle_write_data_to_csv()
        self.eth_bridges_daily_unique_depositors.handle_import_gsc_csv_to_bigquery()

        self.eth_bridges_asset_distribution.save_data()
        self.eth_bridges_asset_distribution.handle_write_data_to_csv()
        self.eth_bridges_asset_distribution.handle_import_gsc_csv_to_bigquery()

        self.eth_bridges_tvl_over_time.save_data()
        self.eth_bridges_tvl_over_time.handle_write_data_to_csv()
        self.eth_bridges_tvl_over_time.handle_import_gsc_csv_to_bigquery()

if __name__ == '__main__':
    a = BridgesScrapyAll()
    a.exec()