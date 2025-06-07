#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

import os
from termcolor import colored
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
from pyspark.sql.window import Window

def get_window(partition_column, initial_row, ending_row, order_by_column="Date"):
  return Window.partitionBy(partition_column).orderBy(order_by_column).rowsBetween(initial_row, ending_row)

def add_window(df, column_name, avg_column, window):
  return df.withColumn(column_name, avg(avg_column).over(window))

def add_signal(df, condition, column_name="Signal"):
  return df.withColumn(column_name, when(condition, 1).otherwise(0))

def to_parquet(df, columns, output_path):
  df[columns].write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":

  input_folder = r"data/ohlc"
  output_folder = r"data/signals"

  spark = SparkSession.builder.appName("Backtastic - Signal Generator").getOrCreate()


  parquet_files = [f for f in os.listdir(input_folder) if f.endswith(".parquet")]

  for file in parquet_files:
    try:
      ticker = file.replace(".parquet", "")
      file_path = os.path.join(input_folder, file)
      print(colored(f"\nProcessing {ticker}...", 'green'))

      df = spark.read.parquet(file_path)
      df = df.withColumn("Date", col("Date").cast("timestamp")).orderBy("Date")
      print(colored(f"  Read parquet of {ticker}...", 'yellow'))

      windows_20 = get_window("Ticker", -19,0)
      windows_50 = get_window("Ticker", -49,0)
      print(colored(f"  Created windows for {ticker}...", 'yellow'))

      df = add_window(df, "SMA_20", "Close", windows_20)
      df = add_window(df, "SMA_50", "Close", windows_50)
      print(colored(f"  Windows added for {ticker}...", 'yellow'))

      df = add_signal(df=df, condition=col("SMA_20") > col("SMA_50"))
      print(colored(f"  Signal added for {ticker}...", 'yellow'))

      output_path = os.path.join(output_folder, f"{ticker}_signals.parquet")

      to_parquet(df, ["Date", "Ticker", "SMA_20", "SMA_50", "Signal"], output_path)
      print(colored((f"  Saved: {output_path}"), 'yellow'))

      print(colored(f"{ticker} Processed!\n", 'green'))

    except Exception as e:
      print(colored(f"Error processing {file}: {e}", 'red'))

  spark.stop()
