"""Module data.py"""
import dask.dataframe as ddf
import numpy as np
import pandas as pd


class Data:
    """
    Data
    """

    def __init__(self):
        """
        Constructor
        """

        self.__fields = {'timestamp': np.int64, 'measure': np.float64}

    def __measures(self, paths: list[str], code: int) -> pd.DataFrame:
        """
        
        :param paths: A list of uniform resource identifiers
        :param code: A ts_id, i.e., a gauge's time series identification code.
        :return: 
        """

        try:
            data: pd.DataFrame = ddf.read_csv(paths, usecols=self.__fields.keys(), dtype=self.__fields).compute()
        except FileNotFoundError as err:
            raise err from err

        data.rename(columns={'measure': str(code)}, inplace=True)
        data.sort_values(by='timestamp', ascending=True, inplace=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)
        data.set_index(keys='timestamp', inplace=True)

        return data

    def exc(self, listing: pd.DataFrame) -> pd.DataFrame:
        """
        
        :param listing: Includes a field of uniform resource identifiers for data acquisition, additionally
                        each instance includes a time series identification code
        :return: 
        """

        codes = listing['ts_id'].unique()

        instances = []
        for code in codes:
            paths = listing.loc[listing['ts_id'] == code, 'uri'].to_list()
            frame = self.__measures(paths=paths, code=code)
            instances.append(frame)
        data = pd.concat(instances, axis=1, ignore_index=False)

        return data
