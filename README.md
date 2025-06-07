# Backtastic - A Fantastic Backtest of a few simple Quantitative Algs"

## How to Use

- Clone the repo
- Execute the following python file (be aware that you might have to use python3 download_data.py) because of the interpreter (or just change the first line to your interpreter):

```python3 download_data.py```


- This will create a folder named data/ohlc and the data from yahoo finance will be there.

- Run the following to generate the signals (1 is for buy, 0 is for hold):

```python3 pyspark/generate_signals.py```

- This will create a folder naned data/signals with the parquet files of the date,ticker,strategies,signal columns.
