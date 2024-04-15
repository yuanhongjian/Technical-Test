import datetime
import pandas as pd

data = [
    {
        "column1": 10,
        "column2": 3.14,
        "column3": {"nested1": "A", "nested2": {"nested3": True}},
        "column4": ["X", "Y"],
        "column5": datetime.datetime(2023, 1, 1),
    },
    {
        "column1": 20,
        "column2": 2.71,
        "column3": {"nested1": "B", "nested2": {"nested3": False}},
        "column4": ["Z"],
        "column5": datetime.datetime(2023, 2, 1),
    },
    {
        "column1": 30,
        "column2": 1.23,
        "column3": {"nested1": "C", "nested2": {"nested3": True}},
        "column4": ["W", "V"],
        "column5": datetime.datetime(2023, 3, 1),
    },
]
df = pd.DataFrame(data)


print(df)
print(df.dtypes)


def generate_bigquery_schema_from_pandas(df: pd.DataFrame) -> list[dict]:
    # Create an empty list to store column name and type
    bigquery_schema = []

    # Create a function to convert nested Series to DataFrame
    def nested_series_to_dataframe(series):
        series_list = []

        for i in series:
            if isinstance(i, dict):
                series_list.append(i)
            else:
                series_list.append({})
        df = pd.DataFrame(series_list)
        return df

    # From DataFrame Type to Bigquery Type
    for column, df_type in zip(df.columns, df.dtypes):
        if df_type == "datetime64[ns]":
            bigquery_schema.append({"name": column, "type": "TIMESTAMP", "mode": "NULLABLE"})
        elif df_type == "int64":
            bigquery_schema.append({"name": column, "type": "INTEGER", "mode": "NULLABLE"})
        elif df_type == "float64":
            bigquery_schema.append({"name": column, "type": "FLOAT", "mode": "NULLABLE"})
        elif df_type == "bool":
            bigquery_schema.append({"name": column, "type": "BOOLEAN", "mode": "NULLABLE"})

        elif df_type == "object":

             #Deal with the REPEATABLE STRING like column4
            if isinstance(df[column][0], list):
                bigquery_schema.append({"name": column, "type": "STRING", "mode": "REPEATABLE"})
            elif isinstance(df[column][0], dict):
             # Deal with nested structure recursively
                nested_schema = generate_bigquery_schema_from_pandas(nested_series_to_dataframe(df[column]))
                if nested_schema:
                    bigquery_schema.append({"name": column, "type": "RECORD", "mode":"NULLABLE", "fields": nested_schema})
                else:
                    bigquery_schema.append({"name": column, "type": "RECORD", "mode":"NULLABLE"})
    return bigquery_schema


for i in generate_bigquery_schema_from_pandas(df):
    print(i)
