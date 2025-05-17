"""Module interface.py"""
import logging

import boto3
import pandas as pd

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.ingress
import src.transfer.cloud
import src.transfer.dictionary
import src.transfer.metadata


class Interface:
    """
    Class Interface
    """

    def __init__(self, connector: boto3.session.Session, service: sr.Service,  s3_parameters: s3p):
        """

        :param service: A suite of services for interacting with Amazon Web Services.
        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__service: sr.Service = service
        self.__s3_parameters: s3p.S3Parameters = s3_parameters

        # Metadata
        metadata = src.transfer.metadata.Metadata(connector=connector)
        self.__points = metadata.exc(name='points.json')
        self.__menu = metadata.exc(name='menu.json')

        # Instances
        self.__configurations = config.Config()
        self.__dictionary = src.transfer.dictionary.Dictionary()

    def __temporary(self, string: str):

        match string:
            case 'menu/annual':
                return self.__menu.get('annual')
            case 'menu/contrasts':
                return self.__menu.get('contrasts')
            case 'points/annual':
                return self.__points.get('annual')
            case 'points/contrasts':
                return self.__points.get('contrasts')
            case _:
                raise ValueError(f'{string} is invalid')

    def exc(self):
        """

        :return:
        """

        # The strings for transferring data to Amazon S3 (Simple Storage Service)
        strings = self.__dictionary.exc(
            path=self.__configurations.measurements_,
            extension='json', prefix=self.__configurations.prefix + '/')
        logging.info(strings[:1])

        strings = strings.assign(
            metadata = strings['section'].apply(lambda x: self.__temporary(x)))
        logging.info(strings)

        '''
        # Adding metadata details per instance
        strings = self.__get_metadata(frame=strings.copy())
        logging.info(strings)


        # Prepare the S3 (Simple Storage Service) section
        src.transfer.cloud.Cloud(
            service=self.__service, s3_parameters=self.__s3_parameters).exc()

        # Transfer
        messages = src.s3.ingress.Ingress(
            service=self.__service, bucket_name=self.__s3_parameters.external).exc(
            strings=strings, tagging='project=hydrography')
        logging.info(messages)
        '''


