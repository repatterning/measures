"""Module data.py"""
import logging

import numpy as np
import pandas as pd

import src.elements.text_attributes as txa
import src.functions.streams


class Data:
    """
    Data
    """

    def __init__(self):
        """
        Constructor
        """

        self.__fields = {'timestamp': np.int64, 'measure': np.float64}

        self.__streams = src.functions.streams.Streams()

    def __measures(self, uri: str, name: str) -> pd.DataFrame:
        """

        :param uri: A uniform resource identifier
        :param name: A first day of the year string, e.g., 2023-01-01.
        :return:
        """

        text = txa.TextAttributes(uri=uri, header=0, usecols=list(self.__fields.keys()), dtype=self.__fields)
        data = self.__streams.read(text=text)

        data.rename(columns={'measure': name}, inplace=True)
        data.sort_values(by='timestamp', ascending=True, inplace=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)
        data.set_index(keys='timestamp', inplace=True)

        return data

    def exc(self, listing: pd.DataFrame):
        """

        :param listing: Includes a field of uniform resource identifiers for data acquisition, additionally
                        each instance includes a time series identification code
        :return:
        """

        listing.info()

        dates = listing['date'].unique()
        logging.info(type(dates[0]))

        instances = []
        for date in dates:
            uri = listing.loc[listing['date'] == date, 'uri'].values[0]
            frame = self.__measures(uri=uri, name=str(date))
            instances.append(frame)
        logging.info(instances)

        return len(instances)