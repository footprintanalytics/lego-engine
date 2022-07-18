from lending.lending_base_model import LendingBaseModel
from utils.constant import CHAIN


class LendingModelAvalanche(LendingBaseModel):
    chain = CHAIN['AVALANCHE']

    def __init__(self):
        if 'avalanche_' not in self.task_name:
            self.task_name = 'avalanche_' + self.task_name
        if 'avalanche_' not in self.project_name:
            self.project_name = 'avalanche_' + self.project_name
        super().__init__()



