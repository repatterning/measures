"""Module splits.py"""
import pandas as pd
import src.split.data


class Splits:
    """
    Splits
    """

    def __init__(self):
        """
        Constructor
        """

        self.__data = src.split.data.Data()


    def exc(self, listing: pd.DataFrame) -> pd.DataFrame:
        """
        The fields of listing are date: pandas.Timestamp, uri: str, catchment_id: int, ts_id: int

        :param listing: Includes a field of uniform resource identifiers for data acquisition, additionally
                        each instance includes a time series identification code
        :return:
        """

        parts = pd.DataFrame()
        for i in range(listing.shape[0]):
            attributes: pd.Series = listing.loc[listing.index[i], :]
            data = self.__data.exc(uri=attributes['uri'], date=attributes['date'])

            if i == 0:
                parts = data
                continue

            parts = parts.merge(data, on='milliseconds', how='outer')

        return parts
