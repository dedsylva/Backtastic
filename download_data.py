#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

import yfinance as yf
import pandas as pd
from pathlib import Path

def download_data(ticker: str, start_date: str, end_date: str, interval: str = "1d") -> pd.DataFrame:
  print(f"Downloading {ticker}")
  data = yf.download(ticker, start=start_date, end=end_date, interval=interval)

  # Flatten multi idex from yfinance
  if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)

  data.reset_index(inplace=True)
  data['Ticker'] = ticker

  return data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]

def to_parquet(df: pd.DataFrame, output_dir: str):
  Path(output_dir).mkdir(parents=True, exist_ok=True)
  file_path = Path(output_dir) / f"{df['Ticker'].iloc[0]}.parquet"
  df["Date"] = df["Date"].astype(str) # spark can't read timestamps with nanoseconds precision
  df.to_parquet(file_path, index=False)
  print(f"Saved {file_path}")

if __name__ == "__main__":
  tickers = ["BBAS3.SA", "PETR3.SA", "BOVA11.SA"]
  data = yf.download(tickers, start="2020-01-01", end="2024-12-31")
  START_DATE = "2020-01-01"
  END_DATE = "2024-12-31"
  for ticker in tickers:
    df = download_data(ticker, start_date=START_DATE, end_date=END_DATE)
    to_parquet(df, output_dir="data")

