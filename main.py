import pandas as pd
from dotenv import load_dotenv
from macro_stress.pipeline import run

load_dotenv()

if __name__ == "__main__":
    end = pd.Timestamp.today().strftime("%Y-%m-%d")
    run(start="2020-01-01", end=end)
