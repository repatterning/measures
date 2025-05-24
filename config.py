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
        self.measures_ = os.path.join(self.warehouse, 'measures')
        self.points_ = os.path.join(self.measures_, 'points')
        self.menu_ = os.path.join(self.measures_, 'menu')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
        self.arguments_key = 'measures/arguments.json'
        self.metadata_ = 'measures/external'

        # The prefix of the Amazon repository where the quantiles will be stored
        self.prefix = 'warehouse/measures'

        # A shift for addressing the absence of 29 February during a common year
        starting = datetime.datetime.strptime('2024-02-29 00:00:00', '%Y-%m-%d %H:%M:%S')
        ending = datetime.datetime.strptime('2024-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        timespan = ending - starting
        timespan.total_seconds()

        self.shift = int(1000 * timespan.total_seconds())

        # The underlying reference year for comparing values - at the same time point across years - is a leap year
        self.leap = '2024-01-01'
