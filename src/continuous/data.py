"""Module data.py"""
import dask.dataframe as ddf
import numpy as np
import pandas as pd

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.prefix


class Data:
    """
    Data
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__service = service
        __s3_parameters: dict = s3_parameters._asdict()
        __s3_arguments: dict = arguments['s3']

        # ...
        self.__intermediary = __s3_parameters[__s3_arguments.get('p_prefix')] + __s3_arguments.get('affix') + '/'

        # Focus
        self.__dtype = {'timestamp': np.float64, 'ts_id': np.float64, 'measure': float}

        # An instance for interacting with objects within an Amazon S3 prefix
        self.__bucket_name = __s3_parameters[__s3_arguments.get('p_bucket')]
        self.__pre = src.s3.prefix.Prefix(service=self.__service, bucket_name=self.__bucket_name)

    def __get_data(self, keys: list[str]):
        """

        :param keys:
        :return:
        """

        try:
            block: pd.DataFrame = ddf.read_csv(
                keys, header=0, usecols=list(self.__dtype.keys()), dtype=self.__dtype).compute()
        except ImportError as err:
            raise err from err

        block.reset_index(drop=True, inplace=True)
        block.sort_values(by='timestamp', ascending=True, inplace=True)
        block.drop_duplicates(subset='timestamp', keep='first', inplace=True)

        return block

    def exc(self, partition: pr.Partitions) -> pd.DataFrame:
        """

        :param partition: Refer to src.elements.partitions
        :return:
        """

        # In focus
        prefix = self.__intermediary + str(partition.catchment_id) + '/' + str(partition.ts_id)

        # Hence
        listings = self.__pre.objects(prefix=prefix)
        keys = [f's3://{self.__bucket_name}/{listing}' for listing in listings]
        block = self.__get_data(keys=keys)

        # Append a date of the format datetime64[]
        # block['datestr'] = pd.to_datetime(block['timestamp'], unit='ms').dt.date

        return block[['timestamp', 'measure']]
