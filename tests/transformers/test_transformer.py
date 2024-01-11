import os
import pytest
import pandas as pd
from src.transformers.transformer import XetraETL

class TestTransformer:

    @pytest.fixture(autouse=True)
    
    def setUp(self):
        self.ExtractETL = XetraETL()
    
        
    def test_is_empty(self):
        
        df = pd.DataFrame(columns=self.ExtractETL.src_columns)

        ret_df = self.ExtractETL.transform_report1(df)

        assert ret_df.empty

    def test_is_returned(self):
        
        df = pd.DataFrame(columns=self.ExtractETL.src_columns)
        
        non_empty_df = pd.concat(
            [df, pd.DataFrame(0, index=range(100), columns=df.columns)],
            ignore_index=True)

        assert not non_empty_df.empty
