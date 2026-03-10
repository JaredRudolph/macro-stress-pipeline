import pandas as pd


def resample_fred(df_fred: pd.DataFrame) -> pd.DataFrame:
    return df_fred.resample("D").last().ffill()


def compute_ratios(df_market: pd.DataFrame) -> pd.DataFrame:
    ratios = pd.DataFrame(index=df_market.index)
    ratios["vix_vix3m"] = df_market["^VIX"] / df_market["^VIX3M"]
    ratios["hyg_lqd"] = df_market["HYG"] / df_market["LQD"]
    ratios["gld_spy"] = df_market["GLD"] / df_market["SPY"]
    ratios["xlk_xlv"] = df_market["XLK"] / df_market["XLV"]
    return ratios


def merge_all(df_market: pd.DataFrame, df_fred: pd.DataFrame) -> pd.DataFrame:
    df_fred_daily = resample_fred(df_fred)
    ratios = compute_ratios(df_market)

    combined = df_market.join(df_fred_daily, how="left")
    combined = combined.join(ratios, how="left", rsuffix="_ratio")
    return combined


if __name__ == "__main__":
    from macro_stress.fetch_data import fetch_market_data, fetch_fred_data

    MARKET_TICKERS = [
        "SPY",  # S&P 500 (overlay only)
        "^VIX",  # VIX
        "^VIX3M",  # VIX 3-month (for ratio)
        "^SKEW",  # SKEW index
        "HYG",  # High yield bonds (for ratio)
        "LQD",  # Investment grade bonds (for ratio)
        "GLD",  # Gold (for ratio)
        "XLK",  # Tech ETF (for ratio)
        "XLV",  # Healthcare ETF (for ratio)
        "DX-Y.NYB",  # DXY dollar index
        "USDCNY=X",  # USD/CNY
    ]

    FRED_SERIES = [
        "T10Y2Y",  # yield curve spread (10Y-2Y)
        "ICSA",  # initial jobless claims
        "CPIAUCSL",  # CPI
        "DRCCLACBS",  # credit card delinquency rate
        "USSLIND",  # leading economic index
    ]

    df_market = fetch_market_data(MARKET_TICKERS, start="2022-01-01", end="2027-01-01")
    df_fred = fetch_fred_data(FRED_SERIES)
    df = merge_all(df_market, df_fred)
    print(df.head())
    print(df.dtypes)
    print(df.isna().sum())
