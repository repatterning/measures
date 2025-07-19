"""Module persist.py"""
import datetime
import json
import os
import time

import pandas as pd

import config
import src.elements.partitions as pr
import src.functions.directories
import src.functions.objects


class Persist:
    """
    Persist
    """

    def __init__(self, reference: pd.DataFrame, frequency: float):
        """

        :param reference: Each instance encodes a few gauge attributes/characteristics
        :param frequency: The granularity of the data, in hours.
        """

        self.__reference = reference
        self.__interval = frequency * 60 * 60 * 1000

        # The storage area
        self.__configurations = config.Config()
        self.__endpoint = os.path.join(self.__configurations.points_, 'continuous')

        # Ensure the storage area exists
        src.functions.directories.Directories().create(self.__endpoint)

        # For creating JSON files
        self.__objects = src.functions.objects.Objects()

    def __get_attributes(self, ts_id: int) -> dict:
        """

        :param ts_id:
        :return:
        """

        frame: pd.DataFrame = self.__reference.loc[self.__reference['ts_id'] == ts_id, :]
        attributes = frame.copy().drop_duplicates(ignore_index=True)

        return attributes.iloc[0, :].to_dict()

    @staticmethod
    def __get_nodes(data: pd.DataFrame) -> dict:
        """

        :param data: The data of a gauge
        :return:
        """

        string = data.copy()['measure'].to_json(orient='split')
        _data = json.loads(string)['data']

        return {'data': _data}

    @staticmethod
    def __get_spreads(spreads: pd.DataFrame) -> dict:
        """

        :param spreads:
        :return:
        """

        spreads.sort_values(by='datestr', ascending=True, inplace=True)
        string = spreads[['low', 'high']].to_json(orient='split')
        _data = json.loads(string)['data']

        # Starting
        minimum: datetime.date = spreads['datestr'].min()
        milliseconds = 1000 * time.mktime(minimum.timetuple())

        return {'data': _data, 'interval': 24 * 60 * 60 * 1000, 'starting': int(milliseconds)}

    def exc(self, data: pd.DataFrame, spreads: pd.DataFrame, partition: pr.Partitions) -> str:
        """

        :param data:
        :param spreads:
        :param partition:
        :return:
        """

        data.sort_values(by='timestamp', inplace=True)

        nodes = self.__get_nodes(data=data)
        nodes['interval'] = self.__interval
        nodes['starting'] = int(data['timestamp'].min())
        nodes['attributes'] = self.__get_attributes(ts_id=partition.ts_id)
        nodes['spreads'] = self.__get_spreads(spreads=spreads.copy())

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__endpoint, f'{partition.ts_id}.json'))

        return message
