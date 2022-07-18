from abc import abstractmethod
import pydash
from utils.constant import PROJECT_PATH


class BaseTemplate:
    project: str = ''
    project_path: str
    temp_type: str = 'base'
    type_list: [str] = []

    @property
    def temp_type_upper(self):
        return pydash.camel_case(self.temp_type)

    def __init__(self, project, chain='ethereum') -> None:
        self.chain = chain
        self.project = project
        self.project_path = f'{PROJECT_PATH}/{self.temp_type}/{chain}/{project}'

    @abstractmethod
    def gen_template(self):
        pass
