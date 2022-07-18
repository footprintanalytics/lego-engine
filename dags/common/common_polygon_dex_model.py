from .common_dex_model import DexModel
from common.common_dex.common_polygon_dex_swap import CommonPolygonDexSwap
from common.common_dex.common_polygon_dex_add_liquidity import CommonPolygonDexAddLiquidity
from common.common_dex.common_polygon_dex_remove_liquidity import CommonPolygonDexRemoveLiquidity


class PolygonDexModel(DexModel):

    def __init__(self):
        super().__init__()

        if 'polygon_' not in self.task_name:
            self.task_name = 'polygon_' + self.task_name
        if 'polygon_' not in self.project_name:
            self.project_name = 'polygon_' + self.project_name

        self.Swap = CommonPolygonDexSwap().init(
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
            self.AddLiquidity = CommonPolygonDexAddLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_add_liquidity',
                self.model_type,
                self.dex_add_liquidity_schema,
                self.source_add_liquidity_sql_file,
                self.history_date
            )
            self.RemoveLiquidity = CommonPolygonDexRemoveLiquidity().init(
                self.project_id,
                self.project_name,
                self.task_name + '_remove_liquidity',
                self.model_type,
                self.dex_remove_liquidity_schema,
                self.source_remove_liquidity_sql_file,
                self.history_date
            )
