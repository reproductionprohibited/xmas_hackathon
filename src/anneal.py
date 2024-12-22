from typing import Callable, List

from conveyor import Conveyor 
from provider import Provider
from payment import Payment
from metrics import Metrics

from random import random
from copy import deepcopy

class Anneal:
    def __init__(self, number_of_args: int):
        self.number_of_args = number_of_args

        self.best: float = 0
        self.best_list: List[float] = []

        self.temperature = 1
        self.temperature_delta = 1 - 1 / (10 ** 1)
        self.number_of_iterations = 500
        self.number_of_allowed_skips = 50
        self.number_of_skips = 0

    def move_point(self, point: List[float]) -> List[float]:
        new_coefs = [0 for i in range(self.number_of_args)]
        for i in range(self.number_of_args):
            new_coefs[i] = point[i] + (random() - 0.5) * self.temperature
        return new_coefs

    def reroll(self) -> List[float]:
        new_coefs = [0 for i in range(self.number_of_args)]
        self.temperature = 1
        self.number_of_skips = 0
        for i in range(self.number_of_args):
            new_coefs[i] = (random() - 0.5) * 10
        return new_coefs

    def calculate_coef(self):
        coefficients = self.reroll()

        metrics_calc = Metrics()
        current_res = 0
        self.best = 0
        self.best_list = deepcopy(coefficients)
        for it in range(self.number_of_iterations):
            temp = self.move_point(coefficients)

            cnv = Conveyor(
                transformed_providers_filename='D:/GitHub/xmas_hackathon/src/files/pkl/providers_transformed.pkl',
                transformed_payments_filename='D:/GitHub/xmas_hackathon/src/files/pkl/payments_transformed.pkl',
            )
            new_res = cnv.check_annealing(temp)

            # providers_list: List[Payment] = []
            # for currency in cnv.pa:
                # for id in cnv.active_providers[currency]:
                #     providers_list.append(cnv.active_providers[currency][id])

            # new_res = Metrics.avg_time(cnv.payment_objs)

            # print(new_res, current_res, self.best, self.number_of_skips, self.temperature)
            # break


            if (new_res > current_res):
                current_res = new_res
                coefficients = deepcopy(temp)
                self.number_of_skips = 0
            elif random() < self.temperature:
                current_res = new_res
                coefficients = deepcopy(temp)

            self.number_of_skips += 1
            
            self.temperature *= self.temperature_delta
            
            if (current_res > self.best):
                self.best = current_res
                self.best_list = deepcopy(coefficients)
            
            if (self.number_of_skips >= self.number_of_allowed_skips):
                coefficients = self.reroll()
                current_res = 0
