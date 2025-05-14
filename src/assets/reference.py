"""Module reference.py"""
import pandas as pd
import numpy as np

import src.elements.s3_parameters as s3p
import src.elements.text_attributes as txa
import src.functions.streams


class Reference:
    """
    Reference
    """

    def __init__(self, s3_parameters: s3p.S3Parameters):
        """

        :param s3_parameters: The overarching S3 (Simple Storage Service) parameters
                              settings of this project, e.g., region code name, buckets, etc.
        """

        self.__s3_parameters: s3p.S3Parameters = s3_parameters
        self.__endpoint = 's3://' + self.__s3_parameters.internal + '/' + 'references' + '/'

        # An instance for reading & writing CSV (comma-separated values) data
        self.__stream = src.functions.streams.Streams()

        # Rename
        self.__rename = {'from': 'starting', 'to': 'until', 'station_latitude': 'latitude',
                         'station_longitude': 'longitude'}

    def __get_reference(self):
        """

        :return:
        """

        uri = self.__endpoint + 'assets.csv'
        usecols = ['station_id', 'station_name', 'catchment_id', 'catchment_name', 'ts_id', 'ts_name',
                   'from', 'to', 'station_latitude', 'station_longitude', 'river_name']
        text = txa.TextAttributes(uri=uri, header=0, usecols=usecols)

        return self.__stream.read(text=text)

    def exc(self, ts_id: np.ndarray) -> pd.DataFrame:
        """

        :param ts_id:
        :return:
        """

        reference = self.__get_reference()
        reference.rename(columns=self.__rename, inplace=True)

        return reference.loc[reference['ts_id'].isin(ts_id), :]
