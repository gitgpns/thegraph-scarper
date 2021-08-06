import pandas as pd

from source.node_scarprer_abc import NodeScarperABC

from pprint import pprint


class UniswapV2Scarper(NodeScarperABC):
    host = 'https://api.thegraph.com/subgraphs/'
    endpoint = 'name/uniswap/uniswap-v2'

    def __init__(self, address, last_timestamp):
        super().__init__(address, last_timestamp)

        self.query = (
                "{"
                f"swaps(skip: {self._skip},orderBy: timestamp, orderDirection: desc, where:"
                "{"
                f'pair: "{self._address}"'
                "}"
                """) {
                     pair {
                       token0 {
                         symbol
                       }
                       token1 {
                         symbol
                       }
                     }
                     amount0In
                     amount0Out
                     amount1In
                     amount1Out
                     amountUSD
                  timestamp
                  transaction {
                    blockNumber
                  }
                 }
                }
            """)

    def _update_parameters(self):
        self._skip += 1000

    def _parse_data(self, data):
        data = data['data']['swaps']
        pprint(data)

        parsed_data = list()

        for elem in data:
            parsed_elem = self._parse_single_elem(elem)

            parsed_data.append(parsed_elem)

        return parsed_data

    @staticmethod
    def _parse_single_elem(elem):
        token0 = elem['pair']['token0']['symbol']
        token1 = elem['pair']['token1']['symbol']

        token0_vol = float(elem['amount0In']) - float(elem['amount0Out'])
        token1_vol = float(elem['amount1In']) - float(elem['amount1Out'])

        timestamp = int(elem['timestamp'])
        block_number = int(elem['transaction']['blockNumber'])

        parsed_elem = [token0, token1, token0_vol, token1_vol, timestamp, block_number]

        return parsed_elem

    def _save_data(self):
        result_table = pd.DataFrame.from_records(data=self._result, columns=self.columns)

        result_table.to_csv(r"C:\Users\lasif\Documents\fond\thegraph-scarper\results\uniswapV2.csv", index=False)
