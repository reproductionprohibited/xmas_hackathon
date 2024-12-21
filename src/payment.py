from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd


@dataclass
class Payment:
    id: str
    timestamp: pd.Timestamp
    amount_usd: float
    currency: str
    card_token: str
    success_probability: float = 0.0
    operation_time: float = 0.0
    comission: float = 0.0

    def __post_init__(self) -> None:
        self.flow: List[int] = []
