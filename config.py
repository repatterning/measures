"""
Module config
"""
import os


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
        self.metadata_ = 'external/metadata/measurements'

        # The prefix of the Amazon repository where the quantiles will be stored
        self.prefix = 'warehouse/measurements'

        # Data granularity
        self.granularity = {'h': 'hour', 'd': 'day', 'w': 'week', 'y': 'year'}
