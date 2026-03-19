# macro-stress-pipeline

A data pipeline that ingests macro and market data, computes a composite financial stress score, and writes the result to parquet for downstream analysis.

The score is a rolling percentile rank (3-year window) averaged across 16 leading indicators, normalized so that **1.0 = maximum stress**. It is built exclusively from indicators that tend to move ahead of broad market conditions, not reactive or coincident ones. The goal is a forward-looking risk signal that leads SPY drawdowns rather than confirming them after the fact.

The stress score is a forward-looking risk indicator built from indicators with demonstrated leading properties. Reactive and coincident indicators (VIX, SKEW, CPI, USD/CNY, gold/equity ratio) were excluded. Optimized weights improve historical fit; they are not a trading signal.

## Indicators

**Yield curve (FRED)**
| Series | Indicator |
|---|---|
| `T10Y2Y` | 10Y-2Y Treasury spread |
| `T10Y3M` | 10Y-3M Treasury spread |
| `T30Y10Y` | 30Y-10Y Treasury spread (computed: `DGS30 - DGS10`) |

**Macro leading (FRED)**
| Series | Indicator |
|---|---|
| `USALOLITOAASTSAM` | OECD composite leading indicator |
| `UMCSENT` | University of Michigan consumer sentiment |
| `PERMIT` | Building permits |
| `NEWORDER` | Manufacturers new orders |

**Labor and credit (FRED)**
| Series | Indicator |
|---|---|
| `ICSA` | Initial jobless claims |
| `DRCCLACBS` | Credit card delinquency rate |
| `BAMLH0A0HYM2` | ICE BofA HY OAS spread |

**Market (yfinance)**
| Ticker | Indicator |
|---|---|
| `XLK` / `XLV` | Tech vs defensive rotation |
| `TLT` | Long-duration Treasury ETF |
| `HG=F` | Copper futures (growth proxy) |
| `CL=F` | Crude oil futures (rate-of-change; dual stress regime) |
| `EEM` | Emerging markets ETF |
| `DX=F` | DXY dollar index |

`CL=F` uses rate-of-change normalization rather than standard rolling percentile rank, since both sharp drops (demand collapse) and sharp spikes (supply shock) represent stress conditions.

## Architecture

```
fetch_data.py    ->    process_data.py    ->    features.py    ->    pipeline.py
yfinance + FRED       merge, resample,        rolling pct rank,   orchestration,
                      compute ratios          composite score     parquet output
```

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/your-username/macro-stress-pipeline.git
cd macro-stress-pipeline
uv sync
```

FRED requires a free API key. Create a `.env` file in the project root:

```
FRED_API_KEY=your_key_here
```

Get a key at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html).

## Usage

```bash
uv run stress-pipeline   # fetch data, compute stress score
```

Outputs:
- `data/raw/market_raw.csv`: raw yfinance closes
- `data/raw/fred_raw.csv`: raw FRED series
- `data/processed/stress_score.parquet`: scored output with all ranked indicators and SPY

## Analysis

`notebooks/stress_score_eda.ipynb` visualizes the pipeline output. Run the pipeline first to generate `data/processed/stress_score.parquet`, then open the notebook in Jupyter or VS Code and run all cells.

**Stress score vs SPY**

![Stress score vs SPY](docs/stress_vs_spy.png)

**Stress score vs SPY drawdown (normalized)**

![Stress score vs SPY drawdown](docs/stress_vs_drawdown.png)

**Indicator breakdown: most recent observation**

![Indicator breakdown](docs/indicator_breakdown.png)

**Individual indicator time series**

![Individual indicators](docs/indicator_grid.png)

## Development

```bash
uv run pytest          # run tests
uv run ruff format .   # format
uv run ruff check .    # lint
```


