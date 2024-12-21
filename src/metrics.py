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
    def sum_amount_of_completed_payments_usd(payments: List[Payment]) -> float:
        total_sum: float = 0.0
        for payment in payments:
            if len(payment.flow) > 0:
                total_sum += payment.amount_usd * payment.success_probability
        
        return total_sum

    @staticmethod
    def avg_time_of_completed_payments_seconds(payments: List[Payment]) -> float:
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
    def median_sum_amount_of_declined_payment_usd(payments: List[Payment]) -> float:
        declined_amounts = []

        for payment in payments:
            if len(payment.flow) == 0:
                declined_amounts.append(payment.amount_usd)

        return np.median(declined_amounts)

    @staticmethod
    def cnt_first_payment_declined_users(payments: List[Payment]) -> float:
        seen: Set[str] = set()
        cnt: int = 0

        for payment in payments:
            if payment.card_token not in seen and len(payment.flow) == 0:
                cnt += 1
            
            seen.add(payment.card_token)
        
        return cnt

    @staticmethod
    def sum_amount_first_payment_declined_payments_usd(payments: List[Payment]) -> float:
        seen: Set[str] = set()
        total_sum: float = 0.0

        for payment in payments:
            if payment.card_token not in seen and len(payment.flow) == 0:
                total_sum += payment.amount_usd
            seen.add(payment.card_token)
        
        return total_sum

    @staticmethod
    def avg_position_of_provider_in_flows(payments: List[Payment]) -> Dict[int, float] :
        provider_indexes = {}
        for payment in payments:
            for index, provider_id in enumerate(payment.flow):
                if provider_id not in provider_indexes:
                    provider_indexes[provider_id] = []
                provider_indexes[provider_id].append(index)
        provider_mean_index = {}
        
        for provider_id in provider_indexes:
            provider_mean_index[provider_id] = np.mean(provider_indexes[provider_id])
        
        return provider_mean_index
    
    @staticmethod
    def provider_load_factor(providers: List[Provider]) -> Dict[int, float]:
        provider_load_factor = {}
        for provider in providers:
            provider_load_factor[provider.id] = provider.payments_sum / provider.limit_max_usd
        
        return provider_load_factor
    
    @staticmethod
    def avg_time(payments: List[Payment]) -> float:
        payment_times = []
        for payment in payments:
            if payment.flow:
                payment_times.append(payment.operation_time)
        
        return np.mean(payment_times)

    @staticmethod
    def total_time(payments: List[Payment]) -> float:
        payment_time = 0
        for payment in payments:
            if payment.flow:
                payment_time += payment.operation_time
        
        return payment_time
    
    @staticmethod
    def avg_total_conversion(payments: List[Payment]) -> float:
        convertion = 0
        for payment in payments:
            convertion += payment.success_probability      
        convertion /= len(payments)
        
        return convertion  

    @staticmethod
    def avg_provided_conversion(payments: List[Payment]) -> float:
        convertion = 0
        payments_count = 0
        for payment in payments:
            if payment.flow:
                convertion += payment.success_probability   
                payments_count += 1   
        convertion /= payments_count

        return convertion  
    
    @staticmethod
    def total_revenue(payments: List[Payment]) -> float:
        profit = 0
        for payment in payments:
            if payment.flow:
                profit += payment.amount_usd - payment.comission
                
        return profit

    @staticmethod
    def total_penalty(providers: List[Provider]) -> float:
        penalty = 0
        
        for provider in providers:
            if provider.payments_sum < provider.limit_min_usd:
                penalty += provider.limit_min_usd / 100
        
        return penalty
    

    def total_profit(self, payments: List[Payment], providers: List[Provider]) -> float:
        return self.total_revenue(payments) - self.total_penalty(providers)


def get_all_metrics(payments: List[Payment], providers_dict: Dict[str, Dict[int, Provider]]) -> None:
    providers_list = []
    for currency in providers_dict:
        for id in providers_dict[currency]:
            providers_list.append(providers_dict[currency][id])

    metrics = Metrics()
    print(f'count_of_completed_payments: {metrics.count_of_completed_payments(payments)}')
    print(f'sum_amount_of_completed_payments_usd: {metrics.sum_amount_of_completed_payments_usd(payments)}')
    print(f'avg_time_of_completed_payments_seconds: {metrics.avg_time_of_completed_payments_seconds(payments)}')
    print(f'median_sum_amount_of_declined_payment_usd: {metrics.median_sum_amount_of_declined_payment_usd(payments)}')
    print(f'cnt_first_payment_declined_users: {metrics.cnt_first_payment_declined_users(payments)}')
    print(f'sum_amount_first_payment_declined_payments_usd: {metrics.sum_amount_first_payment_declined_payments_usd(payments)}')
    # print(f'avg_position_of_provider_in_flows: {metrics.avg_position_of_provider_in_flows(payments)}')
    print(f'provider_load_factor: {metrics.provider_load_factor(providers_list)}')
    print(f'avg_time: {metrics.avg_time(payments)}')
    print(f'total_time: {metrics.total_time(payments)}')
    print(f'avg_total_conversion: {metrics.avg_total_conversion(payments)}')
    print(f'avg_provided_conversion: {metrics.avg_provided_conversion(payments)}')
    print(f'total_penalty: {metrics.total_penalty(providers_list)}')
    print(f'total_profit: {metrics.total_profit(payments, providers_list)}')
    