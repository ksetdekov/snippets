import pandas as pd

def input_json_splitter(input_dict):
    params, df = input_dict['params'], pd.DataFrame(input_dict['dataframe'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return params, df

