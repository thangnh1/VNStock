from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, to_date, expr
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("StockAnalysis") \
    .getOrCreate()

input_path = "gs://vnstock-data/data.csv"

data = spark.read.csv(input_path, header=False, inferSchema=True) \
    .toDF("open", "high", "low", "close", "volume", "tradingdate", "ticker")

end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)

filtered_data = data.filter((col('tradingdate') >= start_date) & (col('tradingdate') <= end_date))

stable_stocks = filtered_data.groupBy("ticker").agg(
    expr("((max(high) - min(low)) / min(low)) as price_volatility"),
    expr("((avg(close) - avg(open)) / avg(open)) as growth_rate")
).filter("growth_rate >= 0").filter("price_volatility <= 0.1")

# stable_stocks = filtered_data.groupBy("ticker").agg(
#     ((avg(col("close")) - avg(col("open"))) / avg(col("open"))).alias("growth_rate"),
#     ((max(col("high")) - min(col("low"))) / min(col("low"))).alias("price_volatility")
# ).filter("growth_rate >= 0").filter("price_volatility <= 0.1")


stable_stocks.write.format("bigquery") \
    .option("writeMethod", "direct") \
    .save("vnstock.stable_vnstock")

spark.stop()

# .option("temporaryGcsBucket", "gs://dataproc-staging-us-east1-543953395582-jma08szm") \
