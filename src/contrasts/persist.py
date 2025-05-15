import json
import os
import logging
import time

import pandas as pd

import config
import src.functions.directories
import src.functions.objects


class Persist:

    def __init__(self, reference: pd.DataFrame):

        self.__reference = reference

        # The storage area
        self.__configurations = config.Config()
        self.__endpoint = os.path.join(self.__configurations.points_, 'contrasts')

        # Ensure the storage area exists
        src.functions.directories.Directories().create(self.__endpoint)

        self.__objects = src.functions.objects.Objects()

    @staticmethod
    def __get_nodes(data: pd.DataFrame) -> dict:

        names = []
        blocks = []
        for column in data.columns:
            string = data[column].to_json(orient='split')
            dictionary = json.loads(string)
            names.append(dictionary['name'])
            blocks.append(dictionary['data'])

        nodes = {'names': names, 'data': blocks}

        return nodes

    def __get_attributes(self, catchment_id: int) -> pd.DataFrame:

        attributes: pd.DataFrame = self.__reference.loc[self.__reference['catchment_id'] == catchment_id, :]
        attributes.drop_duplicates(ignore_index=True, inplace=True)
        attributes.set_index(keys='catchment_id', inplace=True)

        return attributes

    def exc(self, data: pd.DataFrame, catchment_id: int) -> str:

        # Point Start
        starting = data.index.min()
        logging.info('%s: %s', starting, type(starting))

        # Nodes
        nodes = self.__get_nodes(data=data.copy())

        # Attributes
        attributes = self.__get_attributes(catchment_id=catchment_id)

        # Hence
        # pattern = '%Y-%m-%d %H:%M:%S'
        # structure = time.strptime(starting.strftime(pattern), pattern)
        # ... = time.mktime(structure)
        nodes['starting'] = int(starting)
        # nodes['interval'] = arguments.get('frequency') * 60 * 60 * 1000
        nodes['attributes'] = attributes.to_dict(orient='split')

        message = self.__objects.write(
            nodes=nodes, path=os.path.join(self.__endpoint, f'{str(catchment_id)}.json'))

        return message
