from conveyor import Conveyor
from etl_processor import ETLProcessor
from metrics import Metrics

from anneal import Anneal


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

    anneal = Anneal(4)

    anneal.calculate_coef()

    print(f"anneal worked: {anneal.best}: {anneal.best_list}")

    # max_abs = 30
    # step = 0.1

    # max_sum = 0

    # for c1 in range(-max_abs, 0):
    #     for c2 in range(0, max_abs):
    #         for c3 in range(0, max_abs):
    #             for c4 in range(0, max_abs):
    #                 cnv = Conveyor(
    #                     transformed_providers_filename='D:/GitHub/xmas_hackathon/src/files/pkl/providers_transformed.pkl',
    #                     transformed_payments_filename='D:/GitHub/xmas_hackathon/src/files/pkl/payments_transformed.pkl',
    #                 )
    #                 max_sum = max(max_sum, cnv.check_annealing([c1 / 10, c2 / 10, c3 / 10, c4 / 10]))

    # cnv = Conveyor(
    #     transformed_providers_filename='D:/GitHub/xmas_hackathon/src/files/pkl/providers_transformed.pkl',
    #     transformed_payments_filename='D:/GitHub/xmas_hackathon/src/files/pkl/payments_transformed.pkl',
    # )
    # cnv.create_flows()

    # Metrics.log_all_metrics(cnv.payment_objs, cnv.active_providers)


if __name__ == '__main__':
    main()
