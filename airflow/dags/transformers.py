import  re
import pandas as pd

class DataTransformer:
    CLEAN_PATTERN = re.compile(r"[^\w\s.,!?;:'\"()\-]")

    def replace_nulls(self, df):
        return df.fillna("-").replace("null", "-")

    def sort_by_create_date(self, df):
        df = df.copy()
        df["at"] = pd.to_datetime(df["at"], errors="coerce")
        return df.sort_values("at")

    def clean_content(self, df):
        df = df.copy()
        df["content"] = df["content"].astype(str).str.replace(self.CLEAN_PATTERN, "", regex=True)
        return df