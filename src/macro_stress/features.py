import pandas as pd


FLIP_COLS = {"HYG_LQD", "XLK_XLV", "T10Y2Y", "USSLIND"}

SCORE_COLS = [
    "^VIX",
    "VIX_VIX3M",
    "^SKEW",
    "HYG_LQD",
    "GLD_SPY",
    "DX-Y.NYB",
    "USDCNY=X",
    "XLK_XLV",
    "T10Y2Y",
    "ICSA",
    "CPIAUCSL",
    "DRCCLACBS",
    "USSLIND",
]


def rolling_percentile_rank(series: pd.Series, window: int = 756) -> pd.Series:
    return series.rolling(window).apply(lambda x: (x < x[-1]).mean(), raw=True)


def flip_direction(series: pd.Series) -> pd.Series:
    return 1 - series


def compute_stress_score(df: pd.DataFrame) -> pd.DataFrame:
    ranked = {}
    for col in SCORE_COLS:
        ranked_col = rolling_percentile_rank(df[col])
        if col in FLIP_COLS:
            ranked_col = flip_direction(ranked_col)
        ranked[col] = ranked_col

    ranked_df = pd.DataFrame(ranked, index=df.index)
    ranked_df["stress_score"] = ranked_df.mean(axis=1)
    return ranked_df


if __name__ == "__main__":
    from macro_stress.fetch_data import fetch_market_data, fetch_fred_data
    from macro_stress.process_data import merge_all

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

    df_market = fetch_market_data(MARKET_TICKERS, start="2020-01-01", end="2024-01-01")
    df_fred = fetch_fred_data(FRED_SERIES)
    df_merged = merge_all(df_market, df_fred)
    df_scored = compute_stress_score(df_merged)

    print(df_scored[["stress_score"]].dropna().tail(10))
    print(f"\nNaN count:\n{df_scored.isna().sum()}")
    print(
        f"\nScore range: {df_scored['stress_score'].min():.2f} - {df_scored['stress_score'].max():.2f}"
    )
