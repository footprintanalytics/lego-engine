from .common_dex_model import DexModel
from common.common_dex.common_arbitrum_dex_swap import CommonArbitrumDexSwap
from common.common_dex.common_arbitrum_dex_add_liquidity import CommonArbitrumDexAddLiquidity
from common.common_dex.common_arbitrum_dex_remove_liquidty import CommonArbitrumDexRemoveLiquidity


class ArbitrumDexModel(DexModel):

    def __init__(self):
        super().__init__()

        if 'arbitrum' not in self.task_name:
            self.task_name = 'arbitrum_' + self.task_name
        if 'arbitrum_' not in self.project_name:
            self.project_name = 'arbitrum_' + self.project_name

        self.Swap = CommonArbitrumDexSwap().init(
            self.project_id,
            self.project_name,
            self.task_name + '_swap',
            self.model_type,
            self.dex_swap_schema,
            self.source_swap_sql_file,
            self.history_date
        )

        if (self.source_remove_liquidity_sql_file is None) or (self.source_add_liquidity_sql_file is None):
            self.skip_liquidity = True

        if self.skip_liquidity is False:
            self.AddLiquidity = CommonArbitrumDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonArbitrumDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )
