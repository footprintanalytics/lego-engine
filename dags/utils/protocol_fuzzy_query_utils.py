from models.defi_protocol_mapping import DefiProtocolMapping
import difflib, pydash

class ProtocolFuzzyQuery:
    def get_match_list_by_id(self, to_match_list:list, to_match_slug, list_len: int, match_ratio:float=1.0):
        '''
        入参:
            to_match_list => 待匹配的元素池；
            to_match_slug => 待匹配的元素
            list_len => 匹配结果列表的长度, 列表越靠前的相似度越高
            match_ratio => 相似度系数 1：一样，范围：1~0 float
        返回：
            match_list => 在待匹配池中与待陪匹配元素相似待元素列表
        '''
        match_list = difflib.get_close_matches(to_match_slug, to_match_list, list_len, match_ratio)
        return match_list

    def get_match_result_cross_two_list(self, to_traverse_list: list, to_match_list: list, list_len:int, match_ratio:float=1.0):
        '''
            入参:
               to_traverse_list => 待遍历的列表
               to_match_list => 待匹配的元素池
               list_len => 匹配结果列表的长度, 列表越靠前的相似度越高
               match_ratio => 相似度系数 1：一样，范围：1~0 float
            返回：
                match_keys_list => 返回有相似结果的列表，结构: [{key1: [value1..]}, key2: [value2..], ..]
                    key => 待遍历列表的元素;
                    value => 在待匹配池中与待陪匹配元素相似待元素列表
        '''
        match_keys_list = []
        for key in to_traverse_list:
            match_list:list = self.get_match_list_by_id(to_match_list, key, list_len, match_ratio)
            if len(match_list) > 0:
                value = {
                    key: match_list
                }
                match_keys_list.append(value)
        return match_keys_list

    def get_match_ratio_cross_two_element(self, to_match_one:str, to_match_two: str):
        '''
            入参：
                to_match_one/to_match_two: 待对比待两个元素
            返回：
                current_ratio：两者相似度
        '''
        current_ratio: float = difflib.SequenceMatcher(None, to_match_one, to_match_two).quick_ratio()
        return current_ratio




