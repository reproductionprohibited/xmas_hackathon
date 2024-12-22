import time
from typing import List

import pandas as pd

from conveyor import Conveyor
from etl_processor import ETLProcessor
from metrics import Metrics
from payment import Payment



def generate_output_csv(payments: List[Payment]) -> None:
    print('Generating output csv...')

    data = []
    for payment in payments:
        chunk = {
            'id': payment.id,
            'timestamp': payment.timestamp,
            'amount_usd': payment.amount_usd,
            'currency': payment.currency,
            'card_token': payment.card_token,
            'flow': '-'.join([str(it) for it in payment.flow])
        }
        data.append(chunk)
    df = pd.DataFrame(data)
    df.to_csv('./files/result/payment_flows_result.csv', index=False)

    print('Generated output csv successfully!\n')


def main():
    print('Started working')
    etl_processor = ETLProcessor(
        payments_filepath='./files/csv/payments_2.csv',
        providers_filepath='./files/csv/providers_2.csv',
        ex_rates_filepath='./files/csv/ex_rates.csv',
    )
    
    print('Transforming files...')

    etl_processor.extract_transform()
    etl_processor.save_pkl_files()

    print('Transformed files!')

    cnv = Conveyor(
        transformed_providers_filename='./files/pkl/providers_transformed.pkl',
        transformed_payments_filename='./files/pkl/payments_transformed.pkl',
    )
    cnv.create_flows()
    generate_output_csv(payments=cnv.payment_objs)

    Metrics.log_all_metrics(cnv.payment_objs, cnv.active_providers)

    time.sleep(60)

if __name__ == '__main__':
    main()
