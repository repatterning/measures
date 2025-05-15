import logging

import numpy as np
import pandas as pd
import dask

import src.elements.partitions as pr
import src.contrasts.data
import src.contrasts.persist

class Interface:

    def __init__(self, listings: pd.DataFrame, reference: pd.DataFrame):

        self.__listings = listings
        self.__reference = reference

    @dask.delayed
    def __get_codes(self, catchment_id) -> pd.DataFrame:

        return self.__listings.loc[
            self.__listings['catchment_id'] == catchment_id, ['ts_id', 'uri']]

    def exc(self, partitions: list[pr.Partitions]):

        catchment_id_ = np.array([partition.catchment_id for partition in partitions])
        catchment_id_ = np.unique(catchment_id_)

        __get_data = dask.delayed(src.contrasts.data.Data().exc)
        __persist = dask.delayed(src.contrasts.persist.Persist(reference=self.__reference).exc)

        computations = []
        for catchment_id in catchment_id_[:2]:
            listing = self.__get_codes(catchment_id=catchment_id)
            data = __get_data(listing=listing)
            message = __persist(data=data, catchment_id=catchment_id)
            computations.append(message)

        messages = dask.compute(computations, scheduler='threads')[0]
        logging.info(messages)
