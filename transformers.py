from functools import  reduce
from pyspark.sql.functions import when, lit, col


class DataFilter():
	def __init__(self):
		pass

	def transform(self, df):
		df = self.__validate_duplicates__(df)
		df = self.__filter_over_range__(df)
		df = self.__validate_null_values__(df)
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


