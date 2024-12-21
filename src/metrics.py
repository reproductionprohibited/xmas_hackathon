from typing import List, Set, Dict

import numpy as np

from payment import Payment
from provider import Provider


class Metrics:

    @staticmethod
    def count_of_completed_payments(payments: List[Payment]) -> int:
        cnt: int = 0
        for payment in payments:
            if payment.flow:
                cnt += 1
        
        return cnt

    @staticmethod
    def sum_amount_of_completed_payments_usd(payments: List[Payment]) -> float:
        total_sum: float = 0.0
        for payment in payments:
            if payment.flow:
                total_sum += payment.amount_usd * payment.success_probability
        
        return total_sum

    @staticmethod
    def avg_time_of_completed_payments_seconds(payments: List[Payment]) -> float:
        total_time: float = 0.0
        cnt: int = 0
        for payment in payments:
            if payment.flow:
                total_time += payment.operation_time
                cnt += 1
        
        if cnt == 0:
            return 0.0
    
        return round(total_time / cnt, 3)

    @staticmethod
    def median_sum_amount_of_declined_payment_usd(payments: List[Payment]) -> float:
        declined_amounts = []

        for payment in payments:
            if payment.flow:
                declined_amounts.append(payment.amount_usd)

        return np.median(declined_amounts)

    @staticmethod
    def cnt_first_payment_declined_users(payments: List[Payment]) -> float:
        seen: Set[str] = set()
        cnt: int = 0

        for payment in payments:
            if payment.card_token not in seen and payment.flow:
                cnt += 1
            
            seen.add(payment.card_token)
        
        return cnt

    @staticmethod
    def sum_amount_first_payment_declined_payments_usd(payments: List[Payment]) -> float:
        seen: Set[str] = set()
        total_sum: float = 0.0

        for payment in payments:
            if payment.card_token not in seen and payment.flow:
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
            provider_load_factor[provider.id] = round(provider.payments_sum / provider.limit_max_usd * 100, 3)
        
        return provider_load_factor
    
    @staticmethod
    def avg_time(payments: List[Payment]) -> float:
        payment_times = []
        for payment in payments:
            if payment.flow:
                payment_times.append(payment.operation_time)
        
        return np.mean(payment_times)
    
    @staticmethod
    def avg_total_conversion(payments: List[Payment]) -> float:
        convertion = 0
        for payment in payments:
            convertion += payment.success_probability      
        convertion /= len(payments)
        
        return convertion  

    @staticmethod
    def avg_provided_conversion(payments: List[Payment]) -> float:
        conversion = 0
        payments_count = 0
        for payment in payments:
            if payment.flow:
                conversion += payment.success_probability   
                payments_count += 1   
        
        if payments_count == 0:
            return 0.0
        
        return round(conversion / payments_count, 3)
    
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

    @staticmethod
    def profit(payments: List[Payment]) -> float:
        return sum([payment.amount_usd - payment.comission for payment in payments])
    
    @staticmethod
    def total_time(payments: List[Payment]) -> float:
        payment_time = 0
        for payment in payments:
            if payment.flow:
                payment_time += payment.operation_time
        
        return payment_time

    @staticmethod
    def total_profit(payments: List[Payment], providers: List[Provider]) -> float:
        return Metrics.total_revenue(payments) - Metrics.total_penalty(providers)

    @staticmethod
    def log_all_metrics(payments: List[Payment], providers_dict: Dict[str, Dict[int, Provider]]) -> None:
        providers_list = []
        for currency in providers_dict:
            for id in providers_dict[currency]:
                providers_list.append(providers_dict[currency][id])

        print('\n>> Metrics <<')
        print(f'Average total conversion (%): {Metrics.avg_total_conversion(payments) * 100:.2f}')
        print(f'Average provided conversion (%): {Metrics.avg_provided_conversion(payments) * 100:.2f}')
        print(f'Total penalty (USD): {Metrics.total_penalty(providers_list):.2f}')
        print(f'Total profit (USD): {Metrics.total_profit(payments, providers_list):.2f}')
        print(f'Total payment wait time (seconds): {Metrics.total_time(payments):.3f}')
        print(f'Average payment wait time (seconds): {Metrics.avg_time(payments):.3f}')

        print('\n' + '-' * 25)

        print(f'Count of completed payments: {Metrics.count_of_completed_payments(payments)}')
        print(f'Total amount of completed payments (USD): {Metrics.sum_amount_of_completed_payments_usd(payments):.2f}')
        print(f'Average time of completed payments (seconds): {Metrics.avg_time_of_completed_payments_seconds(payments):.3f}')
        print(f'Median amount of declined payments (USD): {Metrics.median_sum_amount_of_declined_payment_usd(payments):.2f}')
        print(f'Count of first-payment declined users: {Metrics.cnt_first_payment_declined_users(payments)}')
        print(f'Total amount of first-payment declined payments (USD): {Metrics.sum_amount_first_payment_declined_payments_usd(payments):.2f}')

        print('\n' + '-' * 25)

        print(f'Every provider\'s load (%):')
        sorted_providers_by_loads = list(Metrics.provider_load_factor(providers_list).items())
        sorted_providers_by_loads.sort(key=lambda record: (record[1], record[0]))

        for provider_id, load_percentage in sorted_providers_by_loads:
            print(f'ID: {' ' * (5 - len(str(provider_id))) + str(provider_id)} | Load: {load_percentage:.2f} %')
