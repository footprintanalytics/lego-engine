from .common_dex_model import DexModel
from common.common_dex.common_avalanche_dex_swap import CommonAvalancheDexSwap
from common.common_dex.common_avalanche_dex_add_liquidity import CommonAvalancheDexAddLiquidity
from common.common_dex.common_avalanche_dex_remove_liquidty import CommonAvalancheDexRemoveLiquidity


class AvalancheDexModel(DexModel):

    def __init__(self):
        super().__init__()

        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name

        self.Swap = CommonAvalancheDexSwap().init(
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
            self.AddLiquidity = CommonAvalancheDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonAvalancheDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )
