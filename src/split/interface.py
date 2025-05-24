"""Module interface.py"""
import logging

import dask
import numpy as np
import pandas as pd

import src.elements.partitions as pr
import src.split.splits
import src.split.persist


class Interface:
    """
    Interface
    """

    def __init__(self, listings: pd.DataFrame, reference: pd.DataFrame, arguments: dict):
        """

        :param listings: Includes a field of uniform resource identifiers for data acquisition, additionally
                         each instance includes a time series identification code
        :param reference: Each instance encodes a few gauge attributes/characteristics
        :param arguments: A set of arguments vis-à-vis calculation objectives.
        """

        self.__listings = listings
        self.__reference = reference
        self.__arguments = arguments

    @dask.delayed
    def __get_codes(self, ts_id) -> pd.DataFrame:
        """

        :param ts_id:
        :return:
        """

        return self.__listings.loc[
            self.__listings['ts_id'] == ts_id, :]

    def exc(self, partitions: list[pr.Partitions]):
        """

        :param partitions: Refer to src/elements/partitions.py for more about Partitions
        :return:
        """

        ts_id_ = np.unique(
            np.array([partition.ts_id for partition in partitions])
        )

        # Delayed Tasks
        __get_splits = dask.delayed(src.split.splits.Splits().exc)
        __persist = dask.delayed(src.split.persist.Persist(
            reference=self.__reference, frequency=self.__arguments.get('frequency')).exc)

        # Compute: Each gauge has a data set/file per year
        computations = []
        for ts_id in ts_id_:
            listing = self.__get_codes(ts_id)
            splits = __get_splits(listing=listing)
            message = __persist(splits=splits, ts_id=ts_id)
            computations.append(message)

        messages = dask.compute(computations, scheduler='threads')[0]
        logging.info(messages)
