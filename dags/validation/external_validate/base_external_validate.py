
from ..base_validate import BaseValidate
import pydash


class BaseExternalValidate(BaseValidate):
    validate_type = 'external'
    project_id = 'footprint-etl-internal'

    def __init__(self, cfg):
        super().__init__(cfg)
        if not pydash.get(cfg, 'protocol_id'):
            raise Exception('protocol_id need input')
        self.protocol_id = str(pydash.get(cfg, 'protocol_id'))

    # 开始校验
    def validate_process(self):
        outside_data = self.get_external_data()
        self_data = self.get_self_data()
        result = self.check_data(outside_data, self_data)

        return result

    # 获取外部数据
    def get_external_data(self):
        pass

    # 获取内部对比数据
    def get_self_data(self):
        pass

    # 对比内外部数据
    def check_data(self, external_data: dict, self_data: dict):
        pass