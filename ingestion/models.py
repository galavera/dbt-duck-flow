from pydantic import BaseModel, Field
from typing import List, Optional


class FredParameters(BaseModel):
    start_date: str = "2020-01-01"
    end_date: str = "2021-01-01"
    variable: str = Field(default="Delinquency Rate on Single-Family")
