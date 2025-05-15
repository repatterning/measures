
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

        :param listings:
        :param reference:
        :param arguments:
        """

        self.__listings = listings
        self.__reference = reference
        self.__arguments = arguments

    @dask.delayed
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

        # Delayed Tasks
        __get_data = dask.delayed(src.contrasts.data.Data().exc)
        __persist = dask.delayed(src.contrasts.persist.Persist(
            reference=self.__reference, frequency=self.__arguments.get('frequency')).exc)

        # Compute
        computations = []
        for catchment_id in catchment_id_[:8]:
            listing = self.__get_codes(catchment_id=catchment_id)
            data = __get_data(listing=listing)
            message = __persist(data=data, catchment_id=catchment_id)
            computations.append(message)

        messages = dask.compute(computations, scheduler='threads')[0]
        logging.info(messages)
