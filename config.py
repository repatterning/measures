"""
Module config
"""
import os
import datetime


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.measurements_ = os.path.join(self.warehouse, 'measurements')
        self.points_ = os.path.join(self.measurements_, 'points')
        self.menu_ = os.path.join(self.measurements_, 'menu')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
        self.arguments_key = 'measurements/arguments.json'
        self.metadata_ = 'measurements/external'

        # The prefix of the Amazon repository where the quantiles will be stored
        self.prefix = 'warehouse/measurements'

        # Times
        starting = datetime.datetime.strptime('2024-02-29 00:00:00', '%Y-%m-%d %H:%M:%S')
        ending = datetime.datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        timespan = ending - starting
        timespan.total_seconds()

        self.shift = int(1000 * timespan.total_seconds())

        self.leap = '2024-01-01'
