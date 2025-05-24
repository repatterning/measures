"""Module data.py"""
import datetime

import numpy as np
import pandas as pd

import config
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

        # Configurations
        self.__configurations = config.Config()

        # An instance for reading CSV files
        self.__streams = src.functions.streams.Streams()

    def __get_data(self, uri: str) -> pd.DataFrame:
        """

        :param uri: A uniform resource identifier

        :return:
        """

        text = txa.TextAttributes(uri=uri, header=0, usecols=list(self.__fields.keys()), dtype=self.__fields)
        data = self.__streams.read(text=text)
        data.sort_values(by='timestamp', ascending=True, inplace=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)
        data['date'] = pd.to_datetime(data['timestamp'], unit='ms')

        return data

    def __get_milliseconds(self, blob: pd.DataFrame, date: pd.Timestamp):
        """

        :param blob:
        :param date:
        :return:
        """

        datestr = date.strftime('%Y-%m-%d')

        if (date.year % 4 ) == 0:
            blob['milliseconds'] = (blob['date'] - pd.Timestamp(datestr)) // pd.Timedelta("0.001s")
        else:
            boundary = datetime.datetime.strptime(f'{date.year}-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
            states = (blob['date'] >= boundary).astype(int).values

            blob['r_milliseconds'] = (blob['date'] - pd.Timestamp(datestr)) // pd.Timedelta("0.001s")
            blob = blob.assign(milliseconds = blob['r_milliseconds'].values + (self.__configurations.shift * states) )

        return blob

    @staticmethod
    def __limits(blob: pd.DataFrame, date: pd.Timestamp):
        """

        :param blob:
        :param date:
        :return:
        """

        lower = datetime.datetime.strptime(f'{date.year}-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        upper = datetime.datetime.strptime(f'{date.year}-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')

        return blob.loc[(blob['date'] >= lower) & (blob['date'] <= upper), :]

    def exc(self, uri: str, date: pd.Timestamp):
        """

        :param uri: A uniform resource identifier
        :param date: A first day of the year string, e.g., 2023-01-01.
        :return:
        """

        data = self.__get_data(uri=uri)
        data = self.__get_milliseconds(blob=data.copy(), date=date)
        data = self.__limits(blob=data.copy(), date=date)

        instances = data.copy()[['milliseconds', 'measure']]
        instances.rename(columns={'measure': str(date.date())}, inplace=True)

        return instances
