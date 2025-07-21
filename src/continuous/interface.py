"""Module interface.py"""
import logging

import dask
import pandas as pd

import src.elements.partitions as pr
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.continuous.data
import src.continuous.persist


class Interface:
    """
    The interface to quantiles calculations.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, arguments: dict):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        :param arguments: A set of arguments vis-à-vis calculation & storage objectives.
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__arguments = arguments

    def exc(self, partitions: list[pr.Partitions], reference: pd.DataFrame):
        """

        :param partitions: The time series partitions.
        :param reference: The reference sheet of gauges.  Each instance encodes the attributes of a gauge.
        :return:
        """

        # Delayed tasks
        __data = dask.delayed(src.continuous.data.Data(
            service=self.__service, s3_parameters=self.__s3_parameters, arguments=self.__arguments).exc)
        __persist = dask.delayed(src.continuous.persist.Persist(
            reference=reference, frequency=self.__arguments.get('frequency')).exc)

        # Compute
        computations = []
        for partition in partitions:
            data = __data(partition=partition)
            message = __persist(data=data, partition=partition)
            computations.append(message)
        messages = dask.compute(computations, scheduler='threads')[0]

        logging.info(messages)
