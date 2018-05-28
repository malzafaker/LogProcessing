#!/usr/bin/env python3
import os
import datetime
import pandas as pd

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


def parsing_logs():
    cohort = {
        'start': '02.05.2016 00:00:00',
        'end': '09.05.2016 23:59:59'
    }  # когорта
    game_identifier = 2  # идентификатор игры
    # installs_csv_path = "files/installs.csv"
    purchases_csv_path = "files/purchases.csv"
    rpi_range = range(1, 11)

    # Проверка заданных параметров
    # is_exists(installs_csv_path)
    is_exists(purchases_csv_path)
    start_cohort = str_in_time(cohort.get('start', ''))
    end_cohort = str_in_time(cohort.get('end', ''))

    data = {}
    purchases_df = pd.read_csv(purchases_csv_path, index_col='install_date', parse_dates=True, iterator=True, sep=',')
    for df in purchases_df:
        df = df.loc[start_cohort:end_cohort]  # Фильтруем по дате
        df = df.loc[df.mobile_app == game_identifier]  # Отбираем данные по нужному приложению

        country_count = df.groupby(['country'])['revenue'].count().sort_values(ascending=False)\
            .reset_index(name='installs')  # Кол-во установок в каждой стране

        for index, row in country_count.iterrows():  # Базовый словарь для обработанных данных
            country = row['country']
            data.setdefault(country, {
                'installs': row['installs']
            })

        df = df.reset_index('install_date')
        date_created = df['created'].values.astype('datetime64')
        install_date = df['install_date'].values.astype('datetime64')
        df['diff'] = date_created - install_date  # Сколько прошло с момента установки игры и покупки

        for rpi in rpi_range:  # Вычисление выручки по количеству суток
            period = df.loc[lambda x: x['diff'] <= pd.np.timedelta64(rpi, 'D'), :]
            country_df = period.groupby(['country'])[['revenue', 'country']].mean()
            for country, row in country_df.iterrows():
                rpi_index = 'RPI%s' % rpi
                data[country][rpi_index] = "%.1f" % row['revenue']

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
