import requests
from abc import abstractmethod
import math

class EtlToTags:

    @abstractmethod
    def origin_data(self):
        pass

    def etl_all(self, origin_all_data):
        return list(map(lambda item: self.etl(item), origin_all_data))

    @abstractmethod
    def etl(self, origin_data) -> {'tag_name': str, 'entity_type_ns_name': str, 'entity_ns_name': str, 'entity_id': str}:
        pass

    def add_tag(self, etl_list: [{'tag_name': str, 'entity_type_ns_name': str, 'entity_ns_name': str, 'entity_id': str}]):

        # 测试标签库
        tag_host = 'http://host'
        limit = 5000
        times = math.ceil(len(etl_list) / limit)
        print('保存标签 总数:{}'.format(len(etl_list)))
        for num in range(0, times):
            print('上传标签: {} {}'.format(num * limit, (num + 1) * limit))
            requests.post(tag_host + '/api/v1/entityTag/tagging', json={
                'opId': 'defi_up',
                'entityTagList': list(map(lambda item: {
                    'tagName': item['tag_name'],
                    'entityTypeNsName': item['entity_type_ns_name'],
                    'entityNsName': item['entity_ns_name'],
                    'entityId': item['entity_id'],
                }, etl_list[num * limit: (num + 1) * limit]))})
        print('保存标签 done')

    def process(self):
        print('开始执行: ', self.__class__.__name__)
        origin = self.origin_data()
        data = self.etl_all(origin)
        # TODO 统一替换map写在这里
        self.add_tag(data)
        print('完成执行: ', self.__class__.__name__)


