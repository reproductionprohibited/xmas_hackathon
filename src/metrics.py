from typing import List, Set, Dict

import numpy as np

from payment import Payment
from provider import Provider


class Metrics:

    @staticmethod
    def count_of_completed_payments(payments: List[Payment]) -> int:
        cnt: int = 0
        for payment in payments:
            if len(payment.flow) > 0:
                cnt += 1
        return cnt

    @staticmethod
    def sum_amount_of_completed_payments_usd(self, payments: List[Payment]) -> float:
        total_sum: float = 0.0
        for payment in payments:
            if len(payment.flow) > 0:
                total_sum += payment.amount_usd * payment.success_probability
        return total_sum

    @staticmethod
    def avg_time_of_completed_payments_seconds(self, payments: List[Payment]) -> float:
        total_time: float = 0.0
        cnt: int = 0
        for payment in payments:
            if len(payment.flow) > 0:
                total_time += payment.operation_time
                cnt += 1
        
        if cnt == 0:
            return 0.0
    
        return total_time / cnt

    @staticmethod
    def median_sum_amount_of_declined_payment_usd(self, payments: List[Payment]) -> float:
        declined_amounts = []

        for payment in payments:
            if len(payment.flow) == 0:
                declined_amounts.append(payment.amount_usd)

        return np.median(declined_amounts)

    @staticmethod
    def cnt_first_payment_declined_users(self, payments: List[Payment]) -> float:
        seen: Set[str] = set()
        cnt: int = 0

        for payment in payments:
            if payment.card_token not in seen and len(payment.flow) == 0:
                cnt += 1
            
            seen.add(payment.card_token)
        
        return cnt

    @staticmethod
    def sum_amount_first_payment_declined_payments_usd(self, payments: List[Payment]) -> float:
        seen: Set[str] = set()
        total_sum: float = 0.0

        for payment in payments:
            if payment.card_token not in seen and len(payment.flow) == 0:
                total_sum += payment.amount_usd
            seen.add(payment.card_token)
        
        return total_sum

    @staticmethod
    def avg_position_of_provider_in_flows(self, payments: List[Payment]) -> Dict[int, float] :
        provider_indexes = {}
        for payment in payments:
            for index, provider_id in enumerate(payment.flow):
                if provider_id not in provider_indexes:
                    provider_indexes[provider_id] = []
                provider_indexes[provider_id].append(index)
        provider_mean_index = {}
        for provider_id in provider_indexes:
            provider_mean_index[provider_id] = np.meam(provider_indexes[provider_id])
        return provider_mean_index
    
    @staticmethod
    def provider_load_factor(self, providers: List[Provider]) -> Dict[int, float]:
        provider_load_factor = {}
        for provider in providers:
            provider_load_factor[provider.id] = provider.payments_sum / provider.limit_max_usd
        return provider_load_factor

    @staticmethod
    def total_providers_conversion(self, providers: List[Provider], payments: List[Payment]) -> float:
        ...

    @staticmethod
    def profit(self, payments: List[Payment]) -> float:
        return sum([payment.amount_usd - payment.comission for payment in payments])

    @staticmethod
    def system_uptime(self, providers: List[Provider], payments: List[Payment]) -> float:
        ...
