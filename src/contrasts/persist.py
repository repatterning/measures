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
        self.__endpoint = os.path.join(self.__configurations.points_, 'contrasts')

        # Ensure the storage area exists
        src.functions.directories.Directories().create(self.__endpoint)

        # For creating JSON files
        self.__objects = src.functions.objects.Objects()

    @staticmethod
    def __get_nodes(data: pd.DataFrame) -> dict:
        """

        :param data: A frame wherein each field's data is the data of a distinct gauge, and the
                     gauges belong to the same catchment.
        :return:
        """

        names = []
        blocks = []
        for column in data.columns:
            string = data[column].to_json(orient='split')
            dictionary = json.loads(string)
            names.append(int(dictionary['name']))
            blocks.append(dictionary['data'])

        nodes = {'names': names, 'data': blocks}

        return nodes

    def __get_attributes(self, catchment_id: int) -> pd.DataFrame:
        """

        :param catchment_id:
        :return:
        """

        frame: pd.DataFrame = self.__reference.loc[self.__reference['catchment_id'] == catchment_id, :]
        attributes = frame.copy().drop_duplicates(ignore_index=True)

        return attributes.set_index(keys='ts_id')

    def exc(self, data: pd.DataFrame, catchment_id: int) -> str:
        """

        :param data:
        :param catchment_id:
        :return:
        """

        # Nodes
        nodes = self.__get_nodes(data=data.copy())

        # Attributes
        attributes = self.__get_attributes(catchment_id=catchment_id)

        # Hence
        nodes['starting'] = int(data.index.min())
        nodes['interval'] = self.__interval
        nodes['attributes'] = attributes.to_dict(orient='split')

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__endpoint, f'{str(catchment_id)}.json'))

        return message
