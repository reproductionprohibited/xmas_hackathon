from datetime import date, datetime
import time
from typing import Dict, List

import pandas as pd

from min_provider_heap import MinProviderHeap
from payment import Payment
from provider import Provider


BOUND_PAYMENT_SUCCESS_PROBABILITY = 1


def datetime_to_date(dt: datetime) -> date:
    year = dt.year
    month = dt.month
    day = dt.day
    return date(year=year, month=month, day=day)


def debug(info: str, filepath: str) -> None:
    print(info, file=open(filepath, mode='a'))


class Conveyor:
    def __init__(self, transformed_providers_filename: str, transformed_payments_filename: str) -> None:
        self.providers: pd.DataFrame = pd.read_pickle(transformed_providers_filename)
        self.payments: pd.DataFrame = pd.read_pickle(transformed_payments_filename)

        # CURRENCY = 'EUR'

        # self.providers: pd.DataFrame = self.providers[self.providers['currency'] == CURRENCY]
        # self.payments: pd.DataFrame = self.payments[self.payments['currency'] == CURRENCY]

        # print(f'{self.providers.shape=}')
        # print(f'{self.payments.shape=}')

        self.setup()

    def setup(self) -> None:
        self.payment_stats: Dict[str, any] = {}
        self.currency_providers_heapmap: Dict[str, MinProviderHeap] = {}
        self.active_providers: Dict[str, Dict[int, Provider]] = {}
        self.payment_objs: List[Payment] = []

    def update_provider_info(self, provider_data_series: pd.Series) -> None:
        id: int = provider_data_series['id']
        new_timestamp: pd.Timestamp = provider_data_series['timestamp']
        new_conversion: float = provider_data_series['conversion']
        new_avg_time: float = provider_data_series['avg_time']
        new_commission: float = provider_data_series['commission']
        currency: str = provider_data_series['currency']
        new_min_sum_usd: float = provider_data_series['min_sum_usd']
        new_max_sum_usd: float = provider_data_series['max_sum_usd']
        new_limit_min_usd: float = provider_data_series['limit_min_usd']
        new_limit_max_usd: float = provider_data_series['limit_max_usd']
        new_fine_usd: float = provider_data_series['limit_min_usd'] * 0.01

        provider = self.active_providers[currency][id]

        provider.avg_time = new_avg_time
        provider.commission = new_commission
        provider.conversion = new_conversion
        provider.min_sum_usd = new_min_sum_usd
        provider.max_sum_usd = new_max_sum_usd

        if new_timestamp - provider.last_update_limits >= pd.Timedelta(hours=24):
            provider.limit_min_usd = new_limit_min_usd
            provider.limit_max_usd = new_limit_max_usd
            provider.fine_usd = new_fine_usd
            provider.last_update_limits = new_timestamp
        
        provider.update_version()

        self.currency_providers_heapmap[currency].push(provider)

    def get_provider_by_id_and_currency(self, currency: str, id: int) -> Provider | None:
        if currency not in self.active_providers:
            return None
        if id not in self.active_providers[currency]:
            return None
        
        return self.active_providers[currency][id]

    def count_skipped_payments(self, payment_list: List[Payment]) -> int:
        skipped: int = 0
        for payment in payment_list:
            if len(payment.flow) == 0:
                skipped += 1
        return skipped

    def create_flows(self) -> None:
        print("STARTED")
        
        start_time = time.time() * 1000
        
        provider_ptr, payment_ptr = 0, 0

        while payment_ptr < self.payments.shape[0]:
            payment_series: pd.Series = self.payments.iloc[payment_ptr]
            new_payment_object: Payment = Payment(
                id = payment_series['payment'],
                timestamp = payment_series['timestamp'],
                amount_usd = payment_series['amount_usd'],
                card_token = payment_series['cardToken'],
                currency = payment_series['currency'],
            )
            payment_ptr += 1

            while new_payment_object.timestamp >= self.providers.iloc[provider_ptr]['timestamp']:
                provider_series: pd.Series = self.providers.iloc[provider_ptr]
                provider_currency = provider_series['currency']
                provider_id = provider_series['id']

                existing_provider = self.get_provider_by_id_and_currency(currency = provider_currency, id = provider_id)
                if existing_provider is None:
                    # Provider with this currency and id does not exist
                    new_provider_object: Provider = Provider.from_series(information_series = provider_series)
                    if provider_currency not in self.active_providers:
                        self.active_providers[provider_currency] = {provider_id: new_provider_object}
                        self.currency_providers_heapmap[provider_currency] = MinProviderHeap()
                        self.currency_providers_heapmap[provider_currency].push(new_provider_object)
                    else:
                        self.active_providers[provider_currency][provider_id] = new_provider_object
                        self.currency_providers_heapmap[provider_currency].push(new_provider_object)
                else:
                    # Provider with this currency and id does not exists
                    self.update_provider_info(provider_data_series = provider_series)

                provider_ptr += 1

            buffer: List[Provider] = []
            self.payment_objs.append(new_payment_object)
            currency_provider_heap: MinProviderHeap = self.currency_providers_heapmap.get(new_payment_object.currency)
            if currency_provider_heap is None:
                payment_ptr += 1
                continue

            if currency_provider_heap.size() > 0:
                
                while new_payment_object.success_probability < BOUND_PAYMENT_SUCCESS_PROBABILITY:
                    top_heap_provider: Provider = currency_provider_heap.pop()
                    if top_heap_provider is None:
                        break

                    top_heap_provider = self.active_providers[top_heap_provider.currency][top_heap_provider.id]
                    buffer.append(top_heap_provider)

                    if (
                        top_heap_provider.payments_sum + new_payment_object.amount_usd <= top_heap_provider.limit_max_usd
                        and top_heap_provider.min_sum_usd <= new_payment_object.amount_usd <= top_heap_provider.max_sum_usd
                    ):
                        current_probability = (1 - new_payment_object.success_probability) * top_heap_provider.conversion
                        expected_payment_sum = new_payment_object.amount_usd * current_probability
                        new_payment_object.comission += expected_payment_sum * top_heap_provider.commission
                        expected_payment_sum -= expected_payment_sum * top_heap_provider.commission

                        top_heap_provider.payments_sum += expected_payment_sum
                        new_payment_object.success_probability += current_probability
                        new_payment_object.flow.append(top_heap_provider.id)
                        new_payment_object.operation_time += top_heap_provider.avg_time * current_probability

                for buffered_provider in buffer:
                    self.currency_providers_heapmap[buffered_provider.currency].push(buffered_provider)
            
        end_time = time.time() * 1000
        delta_time = end_time - start_time
        print(f'🔥💻😎🤩🍆🍆🍆🍆🍆 Program worked for {delta_time} ms')

        skipped: int = self.count_skipped_payments(payment_list = self.payment_objs)
        print(f'Skipped payments: {skipped}\n')
    
    def debug_info(self) -> None:
        debug(f'Logging providers...', filepath='debugging_WW.txt')
        for provider_currency in self.active_providers.keys():
            debug(f'Currency: {provider_currency}', filepath='debugging_WW.txt')
            currency_providers_map = self.active_providers[provider_currency]
            for provider_id in currency_providers_map.keys():
                provider = currency_providers_map[provider_id]
                debug(f"{provider.id} : {provider.payments_sum} / {provider.limit_max_usd}", filepath='debugging_WW.txt')
        
        debug(f'\nLogging flows...', filepath='debugging_WW.txt')
        for payment in self.payment_objs:
            debug(f"{payment.id}: {payment.amount_usd} | {payment.flow}", filepath='debugging_WW.txt')

    def metric_total_provider_payment_sum(self) -> float:
        total_sum: float = 0.0
        for provider_currency in self.active_providers.keys():    
            currency_providers_map = self.active_providers[provider_currency]
            for provider_id in currency_providers_map.keys():
                provider = currency_providers_map[provider_id]
                total_sum += provider.payments_sum
        
        return total_sum 

        