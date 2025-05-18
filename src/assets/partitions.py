"""Module partitions.py"""
import datetime
import typing

import numpy as np
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame, arguments: dict):
        """

        :param data:
        :param arguments:
        """

        self.__data = data
        self.__arguments = arguments

    def __limits(self):
        """

        :return:
        """

        # The boundaries of the dates; datetime format
        spanning = self.__arguments.get('spanning')
        as_from = datetime.date.today() - datetime.timedelta(days=round(spanning*365))
        starting = datetime.datetime.strptime(f'{as_from.year}-01-01', '%Y-%m-%d')

        _end = datetime.datetime.now().year
        ending = datetime.datetime.strptime(f'{_end}-01-01', '%Y-%m-%d')

        # Create series
        limits = pd.date_range(start=starting, end=ending, freq='YS'
                              ).to_frame(index=False, name='date')
        limits['date'] = pd.to_datetime(limits['date'], format='%Y-%m-%d')

        return limits

    def __details(self) -> pd.DataFrame:
        """

        :return:
        """

        codes = np.unique(np.array(self.__arguments.get('excerpt')))

        if codes.size == 0:
            return  self.__data

        catchments = self.__data.loc[self.__data['ts_id'].isin(codes), 'catchment_id'].unique()
        data = self.__data.copy().loc[self.__data['catchment_id'].isin(catchments), :]

        return data if data.shape[0] > 0 else self.__data

    def exc(self) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        """

        :return:
        """

        # The years in focus, via the year start date, e.g., 2023-01-01
        limits = self.__limits()

        # The data sets details
        details = self.__details()

        # Hence, the data sets in focus vis-à-vis the years in focus
        listings = limits.merge(details, how='left', on='date')
        partitions = listings[['catchment_id', 'ts_id']].drop_duplicates()

        return partitions, listings
