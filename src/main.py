from conveyor import Conveyor
from etl_processor import ETLProcessor
# from metrics import Metrics


def main():
    etl_processor = ETLProcessor(
        payments_filepath='D:/GitHub/xmas_hackathon/src/files/csv/payments_1.csv',
        providers_filepath='D:/GitHub/xmas_hackathon/src/files/csv/providers_1.csv',
        ex_rates_filepath='D:/GitHub/xmas_hackathon/src/files/csv/ex_rates.csv',
    )
    
    print('Transforming files...')

    etl_processor.extract_transform()
    etl_processor.save_pkl_files()

    print('Transformed files!')

    for i in range(4):
        cnv = Conveyor(
            transformed_providers_filename='D:/GitHub/xmas_hackathon/src/files/pkl/providers_transformed.pkl',
            transformed_payments_filename='D:/GitHub/xmas_hackathon/src/files/pkl/payments_transformed.pkl',
        )
        cnv.create_flows()

    # Metrics.log_all_metrics(cnv.payment_objs, cnv.active_providers)


if __name__ == '__main__':
    main()
