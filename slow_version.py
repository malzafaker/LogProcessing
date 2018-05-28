#!/usr/bin/env python3
import csv
import os
import datetime

from utils import unloading_data


def is_exists(file_path):
    """
    Проверяет наличие файла
    :param file_path: путь до файла
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(u'Не удалось открыть файл: {file_path}'.format(file_path=file_path))


def str_in_time(str_time):
    """
    Конвертировании строки в дату
    """
    try:
        return datetime.datetime.strptime(str_time, "%d.%m.%Y %H:%M:%S")
    except ValueError:
        raise ValueError('Ошибка при конвертировании строки в дату')


def str_in_time_csv(str_time):
    """
    Конвертировании строки в дату (из csv)
    """
    try:
        return datetime.datetime.strptime(str_time, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        raise ValueError('Ошибка при конвертировании строки в дату')


def parsing_logs():
    """
    Парсинг логов трекинговой системы
    """
    cohort = {
        'start': '02.05.2016 00:00:00',
        'end': '09.05.2016 23:59:59'
    }  # когорта
    game_identifier = '2'  # идентификатор игры
    # installs_csv_path = "files/installs.csv"
    purchases_csv_path = "files/purchases.csv"
    rpi_range = range(1, 11)
    timer = 24

    # Проверка заданных параметров
    # is_exists(installs_csv_path)
    is_exists(purchases_csv_path)
    start_cohort = str_in_time(cohort.get('start', ''))
    end_cohort = str_in_time(cohort.get('end', ''))

    def get_purchases_data(csv_path):
        with open(csv_path) as csv_file:
            for line in csv.reader(csv_file):
                yield line

    iter_purchases = iter(get_purchases_data(purchases_csv_path))
    next(iter_purchases)  # Пропуск headers
    data = {}
    for row in iter_purchases:
        created, mobile_app, country, install_date, revenue = row
        if mobile_app == game_identifier:
            install_date = str_in_time_csv(install_date)  # конвертируем время
            if start_cohort <= install_date <= end_cohort:
                data.setdefault(country, {})
                data[country]['installs'] = data[country].get('installs', 0) + 1
                created = str_in_time_csv(created)  # конвертируем время
                for rpi_index in rpi_range:  # Сбор выручки по количеству суток
                    period = install_date + datetime.timedelta(hours=timer*rpi_index)
                    if created <= period:
                        rpi_index = 'RPI%s' % rpi_index
                        data[country].setdefault(rpi_index, []).append(float(revenue))

    for key in iter(data.keys()):
        for rpi_index in rpi_range:  # Вычисление выручки по количеству суток
            rpi_index = 'RPI%s' % rpi_index
            revenue_data = data[key][rpi_index]
            data[key][rpi_index] = "%.1f" % (sum(revenue_data) / len(revenue_data))

    # Сортировка по убыванию кол-ва установок
    sorted_data = dict(sorted(data.items(), key=lambda item: item[1]['installs'], reverse=True))
    return sorted_data


if __name__ == "__main__":
    start_func = datetime.datetime.now()
    print('Start func: %s' % start_func)

    data = parsing_logs()
    end_parsing = datetime.datetime.now()
    print('End parsing logs file %s' % end_parsing)
    print('Difference: %s' % (end_parsing - start_func))

    unloading_data(data)
    end_func = datetime.datetime.now()
    print('End func %s' % end_func)
    print('Difference: %s' % (end_func - start_func))
    print('please open the file result.csv')
