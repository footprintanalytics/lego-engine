from .common_dex_model import DexModel
from common.common_dex.common_fantom_dex_swap import CommonFantomDexSwap
from common.common_dex.common_fantom_dex_add_liquidity import CommonFantomDexAddLiquidity
from common.common_dex.common_fantom_dex_remove_liquidty import CommonFantomDexRemoveLiquidity


class FantomDexModel(DexModel):

    def __init__(self):
        super().__init__()

        if 'fantom' not in self.task_name:
            self.task_name = 'fantom_' + self.task_name
        if 'fantom_' not in self.project_name:
            self.project_name = 'fantom_' + self.project_name

        self.Swap = CommonFantomDexSwap().init(
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
            self.AddLiquidity = CommonFantomDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonFantomDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )
