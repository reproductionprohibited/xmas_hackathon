from conveyor import Conveyor
from etl_processor import ETLProcessor


def main():
    etl_processor = ETLProcessor(
        payments_filepath='./files/csv/payments_1.csv',
        providers_filepath='./files/csv/providers_1.csv',
        ex_rates_filepath='./files/csv/ex_rates.csv',
    )
    etl_processor.extract_transform()
    etl_processor.save_pkl_files()

    # cnv = Conveyor(
    #     transformed_providers_filename='./files/pkl/providers_transformed.pkl',
    #     transformed_payments_filename='./files/pkl/payments_transformed.pkl',
    # )
    # cnv.create_flows()
    # cnv.log_flows()
    cnv = Conveyor(
        transformed_providers_filename='./files/pkl/providers_transformed.pkl',
        transformed_payments_filename='./files/pkl/payments_transformed.pkl',
    )
    cnv.create_flows()
    # cnv.debug_info()
    print('Total provider payments sum:', cnv.metric_total_provider_payment_sum())

if __name__ == '__main__':
    main()
