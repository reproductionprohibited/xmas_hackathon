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

    def __lt__(self, other):
        # if max(0, self.limit_min_usd - self.payments_sum) == max(0, other.limit_min_usd - other.payments_sum):
        #     return self.commission > other.commission
        # return self.limit_min_usd - self.payments_sum < other.limit_min_usd - other.payments_sum

        params_self = [
            self.commission,
            min(1, self.payments_sum / self.limit_min_usd),
            self.payments_sum / self.limit_max_usd,
            self.avg_time / 30,
        ]
        params_other = [
            other.commission,
            min(1, other.payments_sum / other.limit_min_usd),
            other.payments_sum / other.limit_max_usd,
            other.avg_time / 30,
        ]
        coeficients = [
            -2,
            3,
            1.5,
            4,
        ]

        coeficients_abs_sum = sum(abs(coef) for coef in coeficients)

        normalized_coeficients = [coef / coeficients_abs_sum for coef in coeficients]

        if len(params_self) != len(coeficients) or len(params_other) != len(coeficients):
            print("Check lengths of arrays in comparator !!!")
            raise SystemExit

        sum_self = 0
        sum_other = 0

        for i in range(len(coeficients)):
            sum_self += params_self[i] * normalized_coeficients[i]
            sum_other += params_other[i] * normalized_coeficients[i]
        return sum_self < sum_other
    
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
