
import logging
import numpy as np
import pandas as pd
import dask.dataframe as ddf
import json


class Data:

    def __init__(self):

        self.__fields = {'timestamp': np.int64, 'measure': np.float64}

    def __measures(self, paths: list[str], code: int):

        try:
            data: pd.DataFrame = ddf.read_csv(paths, usecols=self.__fields.keys(), dtype=self.__fields).compute()
        except FileNotFoundError as err:
            raise err from err

        data.rename(columns={'measure': str(code)}, inplace=True)
        data.sort_values(by='timestamp', ascending=True, inplace=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)
        # data.index = pd.to_datetime(data['timestamp'], unit='ms')
        # data.index.rename('index', inplace=True)
        # data.drop(columns='timestamp', inplace=True)
        data.set_index(keys='timestamp', inplace=True)

        return data

    def exc(self, listing: pd.DataFrame):

        codes = listing['ts_id'].unique()

        readings = []
        for code in codes:
            paths = listing.loc[listing['ts_id'] == code, 'uri'].to_list()
            frame = self.__measures(paths=paths, code=code)
            readings.append(frame)
        data = pd.concat(readings, axis=1, ignore_index=False)

        return data
