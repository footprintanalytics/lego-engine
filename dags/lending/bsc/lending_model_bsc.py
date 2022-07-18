from lending.lending_base_model import LendingBaseModel
from utils.constant import CHAIN


class LendingModelBsc(LendingBaseModel):
    chain = CHAIN['BSC']
    execution_time = '5 3 * * *'

    def __init__(self):
        if 'bsc_' not in self.task_name :
            self.task_name = 'bsc_' + self.task_name
        if 'bsc_' not in self.project_name:
            self.project_name = 'bsc_' + self.project_name
        super().__init__()
