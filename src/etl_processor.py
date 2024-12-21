import pandas as pd


class ETLProcessor:
    def __init__(self, payments_filepath: str, providers_filepath: str, ex_rates_filepath: str) -> None:
        self.ex_rates_filepath = ex_rates_filepath

        self.payments = pd.read_csv(payments_filepath)
        self.providers = pd.read_csv(providers_filepath)
        self.ex_rates = pd.read_csv(ex_rates_filepath)

    def extract_transform(self) -> None:
        self.transform_ex_rates_data()
        self.transform_payments_data()
        self.enrich_providers_data()
        self.drop_rates_columns()

    def drop_rates_columns(self) -> None:
        self.payments.drop(labels=['rate'], axis=1, inplace=True)
        self.providers.drop(labels=['rate'], axis=1, inplace=True)

    def transform_ex_rates_data(self) -> None:
        self.ex_rates.rename({'destination': 'currency'}, axis=1, inplace=True)

    def enrich_providers_data(self) -> None:
        self.providers.rename({it: it.lower() for it in self.providers.columns}, axis=1, inplace=True)
        self.providers.rename({'time': 'timestamp'}, axis=1, inplace=True)
        self.providers.drop(labels=['limit_by_card'], axis=1, inplace=True)

        self.providers['timestamp'] = pd.to_datetime(self.providers['timestamp'])
        self.providers = self.providers.merge(self.ex_rates, how='left', on='currency')

        self.providers['min_sum_usd'] = self.providers['min_sum'] * self.providers['rate']
        self.providers['max_sum_usd'] = self.providers['max_sum'] * self.providers['rate']
        self.providers['limit_min_usd'] = self.providers['limit_min'] * self.providers['rate']
        self.providers['limit_max_usd'] = self.providers['limit_max'] * self.providers['rate']
        self.providers['commission'] *= 0.01

        # self.providers.sort_values(by=['timestamp', 'conversion', 'commission'], ascending=[True, False, True], inplace=True)
        self.providers.sort_values(by=['timestamp'], ascending=True, inplace=True)
        self.providers.reset_index(inplace=True)
        self.providers['fine_usd'] = self.providers['limit_min_usd'] * 0.01
        self.providers.drop(labels=['index', 'min_sum', 'max_sum', 'limit_min', 'limit_max'], axis=1, inplace=True)

    def transform_payments_data(self) -> None:
        self.payments.rename({'cur': 'currency', 'eventTimeRes': 'timestamp'}, axis=1, inplace=True)
        self.payments['timestamp'] = pd.to_datetime(self.payments['timestamp'])
        self.payments = self.payments.merge(self.ex_rates, how='left', on='currency')
        self.payments['amount_usd'] = self.payments['amount'] * self.payments['rate']

        self.payments.sort_values(by=['timestamp', 'amount'], ascending=[True, False], inplace=True)
        self.payments.reset_index(inplace=True)
        self.payments.drop(labels=['index', 'amount'], axis=1, inplace=True)

    def save_pkl_files(self, save_to: str = './files/pkl'):
        self.payments.to_pickle(path=f'{save_to}/payments_transformed.pkl')
        self.providers.to_pickle(path=f'{save_to}/providers_transformed.pkl')
