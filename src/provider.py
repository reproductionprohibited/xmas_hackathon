from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class Provider:
    id: int
    last_update_limits: pd.Timestamp
    conversion: float
    avg_time: float
    commission: float
    currency: str
    min_sum_usd: float
    max_sum_usd: float
    limit_min_usd: float
    limit_max_usd: float
    fine_usd: float
    payments_sum: float = 0.0
    version: int = 0

    coeficients = [
        -3.4294474688828527, 
        0.014278492475952628, 
        0.13466681312735318, 
        0.4726135438995368,
    ]

    def get_value_for_comparasion(self) -> float:
        params = [
            self.commission,
            min(1, self.payments_sum / self.limit_min_usd),
            self.payments_sum / self.limit_max_usd,
            self.avg_time / 30,
        ]

        
        coeficients_abs_sum = sum(abs(coef) for coef in self.coeficients)

        normalized_coeficients = [coef / coeficients_abs_sum for coef in self.coeficients]

        if max(params) > 1.5:
            print("BAD COEFS !!!")
            raise SystemExit

        if len(params) != len(normalized_coeficients) or len(params) != len(normalized_coeficients):
            print("Check lengths of arrays in comparator !!!")
            raise SystemExit

        res = 0

        for i in range(len(normalized_coeficients)):
            res += params[i] * normalized_coeficients[i]

        return res

    def __lt__(self, other):
        return self.get_value_for_comparasion() < other.get_value_for_comparasion()
    
    @staticmethod
    def from_series(information_series: pd.Series):
        return Provider(
            id = information_series['id'],
            last_update_limits = information_series['timestamp'],
            conversion = information_series['conversion'],
            avg_time = information_series['avg_time'],
            commission = information_series['commission'],
            currency = information_series['currency'],
            min_sum_usd = information_series['min_sum_usd'],
            max_sum_usd = information_series['max_sum_usd'],
            limit_min_usd = information_series['limit_min_usd'],
            limit_max_usd = information_series['limit_max_usd'],
            fine_usd = information_series['limit_min_usd'] * 0.01
        )

    def update_version(self) -> None:
        self.version += 1
