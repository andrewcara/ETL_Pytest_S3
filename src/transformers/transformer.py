"""Xetra ETL Component"""
from typing import NamedTuple
import logging
import pandas as pd



class XetraETL:
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """
    
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self.src_columns = ['ISIN', 'Mnemonic', 'Date', 'Time',
            'StartPrice', 'EndPrice', 'MinPrice', 'MaxPrice', 'TradedVolume']
        
    def transform_report1(self, data_frame:pd.DataFrame) -> pd.DataFrame:
        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        self._logger.info('Applying transformations to Xetra source data for report 1 started...')
        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_columns]
        # Removing rows with missing values
        data_frame.dropna(inplace=True)
        self._logger.info('Applying transformations to Xetra source data finished...')
        return data_frame