import csv


def unloading_data(data):
    """
    Запись данных в csv
    :param data: Обработанные данные трекинговой системы
    """
    rpi_range = range(1, 11)

    def get_headers():
        headers = ['country', 'installs']
        for rpi_index in rpi_range:
            headers.append('RPI%s' % rpi_index)
        return headers

    with open('result.csv', 'w') as csv_file:
        out_file = csv.writer(csv_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        out_file.writerow(get_headers())
        for country in iter(data.keys()):
            write_data = [country, data[country]['installs']]
            for rpi_index in rpi_range:
                rpi_index = 'RPI%s' % rpi_index
                write_data.append(data[country][rpi_index])

            out_file.writerow(write_data)
