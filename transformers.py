from pyspark.sql.functions import when, lit, col, year, month, dayofweek, weekofyear, to_timestamp, hour, avg, count, sum
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from functools import reduce

class DataFilter():
    def __init__(self):
        pass

    def transform(self, df):

        df = df.withColumn('tranaction_id',col('tranaction_id').cast('long'))\
                .withColumn('customer_id',col('customer_id').cast('int'))\
                .withColumn('terminal_id',col('terminal_id').cast('int'))\
                .withColumn('tx_amount',col('tx_amount').cast('double'))\
                .withColumn('tx_time_seconds',col('tx_time_seconds').cast('long'))\
                .withColumn('tx_time_days',col('tx_time_days').cast('long'))\
                .withColumn("tx_datetime", to_timestamp("tx_datetime"))


        df = self.__validate_null_values__(df)
        df = self.__validate_duplicates__(df)
        df = self.__filter_over_range__(df)
        df = self.__create_terminal_features__(df)
        df = self.__create_customer_features__(df)
        df = self.__create_time_features__(df)
        return df
    
    def __validate_null_values__(self, df):
        df = df.dropna()
        return df   

    def __validate_duplicates__(self, df):
        df = df.dropDuplicates()
        return df


    def __filter_over_range__(self, df):
        to_convert = set(['tranaction_id', 'customer_id', 'terminal_id', 'tx_time_seconds'])
        df = reduce(lambda df, x: df.withColumn(x, when(col(x) < 0, 0).otherwise(col(x))),
                        to_convert,
                        df)
        return df

    def __create_time_features__(self, df):
        df = df.withColumn("day_of_week", dayofweek("tx_datetime"))
        df = df.withColumn("week_of_year", weekofyear("tx_datetime"))
        df = df.withColumn("is_weekend", when(col("day_of_week") == 1, 1).when(col("day_of_week") == 7,1).otherwise(0))
        df = df.withColumn("hour", hour("tx_datetime"))
        df = df.withColumn("is_day", when(col("hour")<=6, 0).otherwise(1))
        return df

    def __create_customer_features__(self, df, ws_arr = [1, 7, 30]):
        for ws in ws_arr:
            windowPartition = Window.partitionBy("customer_id").orderBy(F.col("tx_datetime").cast('long')).rangeBetween(-ws*86400, 0)
            df = df.withColumn(f"avg_amount_per_customer_for_{ws}_days", avg('tx_amount').over(windowPartition))
            df = df.withColumn(f"number_of_tx_per_customer_for_{ws}_days", count('tx_amount').over(windowPartition))
        return df

    def __create_terminal_features__(self, df, ws_arr = [1, 7, 30]):
        for ws in ws_arr:
            windowPartition = Window.partitionBy("terminal_id").orderBy(F.col("tx_datetime").cast('long')).rangeBetween(-ws*86400, 0)
            df = df.withColumn(f"number_of_tx_per_terminal_for_{ws}_day", count('tx_fraud').over(windowPartition))
            df = df.withColumn(f"number_of_fraud_tx_per_terminal_for_{ws}_day", sum('tx_fraud').over(windowPartition))
        return df

