from lending.lending_base_model import LendingBaseModel
from utils.constant import CHAIN


class LendingModelPolygon(LendingBaseModel):
    chain = CHAIN['POLYGON']

    def __init__(self):
        if 'polygon_' not in self.task_name:
            self.task_name = 'polygon_' + self.task_name
        if 'polygon_' not in self.project_name:
            self.project_name = 'polygon_' + self.project_name
        super().__init__()



