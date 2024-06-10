from pyspark.sql.functions import when, lit, col, year, month, dayofweek, weekofyear, to_timestamp
from functools import reduce

class DataFilter():
    def __init__(self):
        pass

    def transform(self, df):
        df = self.__validate_duplicates__(df)
        df = self.__filter_over_range__(df)
        df = self.__validate_null_values__(df)
        df = self.__create_time_features__(df)
        return df
    
    def __validate_null_values__(self, df, selected_columns = {'terminal_id': 0}):
        for k, v in selected_columns.items():
            df = df.withColumn(k, when(df[k].isNull(), lit(v)).otherwise(df[k]))
        return df   


    def __filter_over_range__(self, df):
        to_convert = set(['tranaction_id', 'customer_id', 'terminal_id', 'tx_time_seconds'])
        df = reduce(lambda df, x: df.withColumn(x, when(col(x) < 0, 0).otherwise(col(x))),
                        to_convert,
                        df)
        return df

    def __validate_duplicates__(self, df):
        df = df.dropDuplicates()
        return df

    def __create_time_features__(self, df):
        df = df.withColumn("year", to_timestamp("tx_datetime"))
        df = df.withColumn("year", year("tx_datetime"))
        df = df.withColumn("month", month("tx_datetime"))
        df = df.withColumn("day_of_week", dayofweek("tx_datetime"))
        df = df.withColumn("week_of_year", weekofyear("tx_datetime"))
        df = df.withColumn("is_weekend", when(col("day_of_week") == 1, 1).when(col("day_of_week") == 7,1).otherwise(0))
        return df