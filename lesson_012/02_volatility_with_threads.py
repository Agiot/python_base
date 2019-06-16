# -*- coding: utf-8 -*-


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПОТОЧНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
# TODO Внимание! это задание можно выполнять только после зачета lesson_012/01_volatility.py !!!

# TODO тут ваш код в многопоточном стиле


import csv
import os
from collections import OrderedDict
from utils import time_track
from threading import Thread

trade_files = '/home/andrey/PycharmProjects/python_base/lesson_012/trades'


class TickerVolatility(Thread):
    tickers = {}

    def __init__(self, file_path, min_str_cnt, max_str_cnt, print_zero_tickers=False):
        super().__init__()
        self.min_str_cnt = min_str_cnt
        self.max_str_cnt = max_str_cnt
        self.print_zero_tickers = print_zero_tickers
        self.file_path = file_path
        self.ticker, self.prices = set(), []
        self.ordered_tickers = {}
        self.zero_volatility = []

    def _get_file_from_file_list(self):
        for dirpath, dirnames, filenames in os.walk(self.file_path):
            for filename in filenames:
                file_name = os.path.join(dirpath, filename)
                # print(self.name, TickerVolatility.tickers.keys())
                with open(file=file_name, mode='r', encoding='utf8') as csv_file:
                    csv_dict = csv.DictReader(csv_file, delimiter=',')
                    self.prices = []
                    try:
                        for line in csv_dict:
                            self.ticker = line['SECID']
                            if self.ticker not in TickerVolatility.tickers.keys():
                                self.prices.append(float(line['PRICE']))
                            else:
                                raise ValueError(f'{self.name} Ticker {self.ticker} уже заполнен')

                        yield self.ticker, self.prices

                    except ValueError as exc:
                        # print(exc)
                        continue

    def calculate_volatility(self):

        for ticker, prices in self._get_file_from_file_list():
            max_price, min_price = max(prices), min(prices)
            average_price = (max_price + min_price) / 2
            volatility = ((max_price - min_price) / average_price) * 100
            TickerVolatility.tickers[self.ticker] = volatility
            # print(f'{self.name} Ticker {self.ticker} добавлен')

        self.ordered_tickers = OrderedDict(sorted(TickerVolatility.tickers.items(), key=lambda x: x[1], reverse=True))
        self.zero_volatility = []

        for k, v in list(self.ordered_tickers.items()):
            if v == 0:
                self.zero_volatility.append(k)
                del self.ordered_tickers[k]

    def print_max_volatility(self):
        i = 0
        for secid, volatility in self.ordered_tickers.items():
            i += 1
            if i <= self.max_str_cnt:
                print(f'\t{secid} - {volatility} %')

    def print_min_volatility(self):
        i = 0
        for secid, volatility in self.ordered_tickers.items():
            i += 1
            if i >= len(self.ordered_tickers.items()) - self.min_str_cnt + 1:
                print(f'\t{secid} - {volatility} %')

    def print_zero_volatility(self):
        if self.print_zero_tickers:
            print(f'\t{sorted(self.zero_volatility)}')

    @time_track
    def run(self):

        self.calculate_volatility()

        if self.max_str_cnt:
            print('Максимальная волатильность:')
            self.print_max_volatility()

        if self.min_str_cnt:
            print('Минимальная волатильность:')
            self.print_min_volatility()

        if self.print_zero_tickers:
            print('Нулевая волатильность:')
            self.print_zero_volatility()


if __name__ == '__main__':
    # report = TickerVolatility(file_path=trade_files,
    #                           min_str_cnt=3,
    #                           max_str_cnt=3,
    #                           print_zero_tickers=True
    #                           )
    # report.start()
    # report.join()

    min_report = TickerVolatility(file_path=trade_files,
                                  min_str_cnt=0,
                                  max_str_cnt=3,
                                  print_zero_tickers=False
                                  )

    max_report = TickerVolatility(file_path=trade_files,
                                  min_str_cnt=3,
                                  max_str_cnt=0,
                                  print_zero_tickers=False
                                  )

    zero_report = TickerVolatility(file_path=trade_files,
                                   min_str_cnt=0,
                                   max_str_cnt=0,
                                   print_zero_tickers=True
                                   )

    min_report.start()
    max_report.start()
    zero_report.start()

    zero_report.join()
    min_report.join()
    max_report.join()

