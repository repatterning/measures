"""Module menu.py"""
import logging
import os

import pandas as pd

import config
import src.functions.objects


class Menu:
    """
    Menu
    """

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()
        self.__objects = src.functions.objects.Objects()

    def __menu(self, reference: pd.DataFrame):
        """

        :param reference:
        :return:
        """

        excerpt = reference.copy().sort_values(by=['catchment_name', 'station_name'], ascending=True)

        # Menu
        names = (excerpt['station_name'] + '/' + excerpt['catchment_name']).to_numpy()
        frame = pd.DataFrame(data={'desc': excerpt['ts_id'].to_numpy(), 'name': names})
        nodes = frame.to_dict(orient='records')

        return self.__objects.write(
            nodes=nodes, path=os.path.join(self.__configurations.menu_, 'menu.json'))


    def exc(self, reference: pd.DataFrame):
        """

        :param reference: The reference sheet of the water level gauges.
        :return:
        """

        message = self.__menu(reference=reference)

        logging.info('Graphing Menu ->\n%s', message)
