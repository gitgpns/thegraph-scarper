import requests
from abc import ABC, abstractmethod
import datetime
import pandas as pd


class NodeScarperABC(ABC):
    api = '41a2f13b447bfaf0ca996c61ec6493b5'

    host = None
    endpoint = None
    query = None
    columns = ['token0', 'token1', 'token0_vol', 'token1_vol', 'timestamp', 'block_number']

    def __init__(self, address, last_timestamp):
        self._address = address
        self._last_timestamp = last_timestamp
        self._current_last_timestamp = self._get_current_timestamp()

        self._parameters = None
        self._result = None
        self._skip = 0

    @staticmethod
    def _get_current_timestamp():
        current_datetime = datetime.datetime.utcnow().timestamp()

        return current_datetime

    def scarp_data(self):
        self._create_url()

        self._init_result()

        while self._check_if_data_is_needed():
            parameters = self._update_parameters()

            data = self._query_data(parameters)

            parsed_data = self._parse_data(data)

            self._add_to_table(parsed_data)

            self._update_last_timestamp(parsed_data)

        self._save_data()

    def _check_if_data_is_needed(self):
        if self._current_last_timestamp <= self._last_timestamp:
            return False

        return True

    def _create_url(self):
        self._url = self.host + self.endpoint

    def _init_result(self):
        self._result = list()

    def _query_data(self, parameters):
        request = requests.post(
            self._url,
            '',
            json={'query': self.query, 'variables': parameters}
        )

        if request.status_code == 200:
            return request.json()
        else:
            raise Exception('Query failed. return code is {}.      {}'.format(request.status_code, self.query))

    def _update_last_timestamp(self, parsed_data):
        last_date = parsed_data[-1][4]
        self._current_last_timestamp = last_date

    @abstractmethod
    def _update_parameters(self):
        pass

    @abstractmethod
    def _parse_data(self, data):
        pass

    def _add_to_table(self, parsed_data):
        self._result.extend(parsed_data)

    @abstractmethod
    def _save_data(self):
        pass
