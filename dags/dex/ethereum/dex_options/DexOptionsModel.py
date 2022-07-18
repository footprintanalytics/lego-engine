from common.common_dex_model import DexModel


class DexOptionModel(DexModel):
    source_liquidity_sql_file = 'dex/ethereum/dex_options/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/dex_options/swap.sql'
    options = {
        'project': '',
        'events': [
            {'name': 'mint', 'value': 'blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Mint'},
            {'name': 'pair_created', 'value': 'blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated'},
            {'name': 'swap', 'value': 'blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap'},
            {'name': 'burn', 'value': 'blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Burn'}
        ]
    }

    def build_liquidity_source_sql(self, match_date_filter: str):
        sql = super().build_liquidity_source_sql(match_date_filter)
        # 只能验证可行，具体替换逻辑需要优化。
        print('source_liquidity_sql_file query_string', sql)
        sql = sql.replace('{project}', self.options['project'])
        return self.replace_event_values(sql)

        # 生成swap的sql

    def replace_event_values(self, sql):
        for event in self.options['events']:
            sql = sql.replace('{' + event['name'] + '}', event['value'])
        return sql

    def build_swap_source_sql(self, match_date_filter: str):
        source = self.get_sql_from_file(
            self.source_swap_sql_file,
            match_date_filter
        )
        source = source.replace('{project}', self.options['project'])
        return self.replace_event_values("""
                select
                    DATETIME(dexs.block_time) AS block_time,
                    tokena.symbol AS token_a_symbol,
                    tokenb.symbol AS token_b_symbol,
                    SAFE_DIVIDE(token_a_amount_raw, POW(10, tokena.decimals)) AS token_a_amount,
                    SAFE_DIVIDE(token_b_amount_raw, POW(10, tokenb.decimals)) AS token_b_amount,
                    project,
                    version,
                    category,
                    tx.from_address AS trader_a,
                    tx.to_address AS trader_b,
                    token_a_amount_raw,
                    token_b_amount_raw,
                    token_a_address,
                    token_b_address,
                    exchange_contract_address,
                    tx_hash,
                    tx.from_address AS tx_from,
                    tx.to_address AS tx_to,
                    CAST(trace_address AS STRING) AS trace_address
                from (
                    {source}
                ) dexs
                inner join `bigquery-public-data.crypto_ethereum.transactions` tx
                on dexs.tx_hash = tx.hash and DATE(tx.block_timestamp) {match_date_filter}
                left join `xed-project-237404.footprint_etl.erc20_tokens` tokena
                on Lower(tokena.contract_address) = dexs.token_a_address
                left join `xed-project-237404.footprint_etl.erc20_tokens` tokenb
                on Lower(tokenb.contract_address) = dexs.token_b_address
                """.format(
            source=source,
            match_date_filter=match_date_filter
        ))
