from typing import Union, List

import pandas


def split_dataframe_by_chunk(data: Union[pandas.DataFrame, pandas.Series], chunk_size: int) -> List[
    Union[pandas.DataFrame,
          pandas.Series]]:
    return [data.iloc[i:i + chunk_size]
            for i in range(0, data.shape[0], chunk_size)]
