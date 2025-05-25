"""Module persist.py"""
import json
import os

import pandas as pd

import config
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
        self.__endpoint = os.path.join(self.__configurations.points_, 'split')

        # Ensure the storage area exists
        src.functions.directories.Directories().create(self.__endpoint)

        # For creating JSON files
        self.__objects = src.functions.objects.Objects()

    @staticmethod
    def __get_nodes(data: pd.DataFrame) -> dict:
        """

        :param data: The data of a gauge
        :return:
        """

        frame = data.copy().drop(columns='milliseconds')

        dictionaries = [json.loads(data[column].to_json(orient='split')) for column in frame.columns]
        periods = [dictionaries[i]['name'] for i in range(len(dictionaries))]
        sections = [dictionaries[i]['data'] for i in range(len(dictionaries))]

        nodes = {'periods': periods, 'data': sections}

        return nodes

    def __get_attributes(self, ts_id: int) -> pd.DataFrame:
        """

        :param ts_id:
        :return:
        """

        frame: pd.DataFrame = self.__reference.loc[self.__reference['ts_id'] == ts_id, :]
        attributes: pd.DataFrame = frame.copy().drop_duplicates(ignore_index=True)

        return attributes

    def exc(self, splits: pd.DataFrame, ts_id: int) -> str:
        """

        :param splits:
        :param ts_id:
        :return:
        """

        # Nodes vis-à-vis the data fields only
        nodes = self.__get_nodes(data=splits.copy())

        # Attributes
        attributes = self.__get_attributes(ts_id=ts_id)

        # Hence
        nodes['starting'] = self.__configurations.leap
        nodes['interval'] = self.__interval
        nodes['attributes'] = attributes.iloc[0, :].to_dict()

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__endpoint, f'{str(ts_id)}.json'))

        return message
