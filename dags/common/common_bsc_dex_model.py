from .common_dex_model import DexModel
from common.common_dex.common_bsc_dex_swap import CommonBscDexSwap
from common.common_dex.common_bsc_dex_add_liquidity import CommonBscDexAddLiquidity
from common.common_dex.common_bsc_dex_remove_liquidity import CommonBscDexRemoveLiquidity


class DexBSCModel(DexModel):
    # execution_time = '0 6 * * *'

    def __init__(self):
        super().__init__()

        if 'bsc_' not in self.task_name:
            self.task_name = 'bsc_' + self.task_name
        if 'bsc_' not in self.project_name:
            self.project_name = 'bsc_' + self.project_name

        self.Swap = CommonBscDexSwap().init(
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
            self.AddLiquidity = CommonBscDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonBscDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )
