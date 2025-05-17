"""Module interface.py"""
import logging

import dask
import numpy as np
import pandas as pd

import src.contrasts.data
import src.contrasts.persist
import src.elements.partitions as pr


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

    def __get_codes(self, catchment_id) -> pd.DataFrame:
        """

        :param catchment_id:
        :return:
        """

        return self.__listings.loc[
            self.__listings['catchment_id'] == catchment_id, ['ts_id', 'uri']]

    def exc(self, partitions: list[pr.Partitions]):
        """

        :param partitions: Refer to src/elements/partitions.py for more about Partitions
        :return:
        """

        catchment_id_ = np.array([partition.catchment_id for partition in partitions])
        catchment_id_ = np.unique(catchment_id_)

        # Tasks
        __get_data = src.contrasts.data.Data()
        __persist = src.contrasts.persist.Persist(
            reference=self.__reference, frequency=self.__arguments.get('frequency'))

        # Compute: There are fewer than fifty catchments, but there are between 1 ≤ gauges per catchment ≤ 50.  Therefore,
        # use dask.compute within the data class
        computations = []
        for catchment_id in catchment_id_:
            listing = self.__get_codes(catchment_id=catchment_id)
            data = __get_data.exc(listing=listing)
            message = __persist.exc(data=data, catchment_id=catchment_id)
            computations.append(message)

        logging.info(computations)
