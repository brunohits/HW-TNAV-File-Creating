import subprocess  # Импорт модуля для запуска внешних процессов
import pandas as pd  # Импорт библиотеки Pandas для работы с табличными данными
import chardet  # Импорт модуля для определения кодировки файла
import csv  # Импорт модуля для работы с CSV-файлами
import locale  # Импорт модуля для работы с локализацией

# Запрос пользовательского ввода пути к файлам
inclination_path = input("Введите путь к файлу инклинометрии (нажмите Enter для использования пути по умолчанию): ")
trajectory_path = input("Введите путь к файлу траектории (нажмите Enter для использования пути по умолчанию): ")
tp_path = input("Введите путь к файлу TР (нажмите Enter для использования пути по умолчанию): ")
perf_path = input("Введите путь к файлу перфораций (нажмите Enter для использования пути по умолчанию): ")

# Использование пути по умолчанию, если пользователь не ввел ничего
if not inclination_path:
    inclination_path = 'Инклинометрия_НП4.csv'
if not trajectory_path:
    trajectory_path = 'Траектория_НП4.csv'
if not tp_path:
    tp_path = 'ТР.csv'
if not perf_path:
    perf_path = 'Perf_NP4.csv'

# Чтение файлов с использованием предоставленных путей
df_inclin = pd.read_csv(inclination_path)
df_trajectory = pd.read_csv(trajectory_path)
df_tp = pd.read_csv(tp_path)
df_perf = pd.read_csv(perf_path)


def process_welltrack(df_inclin, df_trajectory):  # Функция для обработки данных welltrack

    merged_df = pd.concat([df_inclin, df_trajectory], axis=1)  # Объединение DataFrame'ов по столбцам

    with open('output_WellTracks.txt', 'w', encoding='utf-8') as file:  # Открытие файла для записи
        prev_well = None  # Переменная для хранения предыдущего названия скважины
        for index, row in merged_df.iterrows():  # Цикл по строкам объединенного DataFrame
            well = row['Скважина'].iloc[0]  # Получение названия скважины
            data = row.drop('Скважина')  # Удаление столбца "Скважина" из строки

            if prev_well is None or well != prev_well:  # Если скважина изменилась
                if prev_well is not None:  # Если предыдущая скважина существует
                    file.seek(file.tell() - 2)  # Перемещение указателя файла на 2 байта назад
                    file.write('\t/\n/\n')  # Запись разделителя между скважинами

                file.write(f'WELLTRACK "{well}"\n')  # Запись названия скважины в файл
                prev_well = well  # Обновление предыдущей скважины

            row = f'{row["X"]}\t{row["Y"]}\t{row["Zабс"]}\t{row["Глубина"]}'.replace(",",
                                                                                     ".")  # Форматирование строки данных

            file.write(row + '\n')  # Запись строки данных в файл

        file.seek(file.tell() - 2)  # Перемещение указателя файла на 2 байта назад
        file.write('\t/\n')  # Запись разделителя в конце файла
        file.write('/')  # Запись разделителя в конце файла


def process_welspecs(tp_path):  # Функция для обработки данных welspecs

    with open(tp_path, 'rb') as f:  # Открытие входного файла для чтения в байтовом режиме
        encoding = chardet.detect(f.read())['encoding']  # Определение кодировки файла

    df = pd.read_csv(tp_path, sep=',', encoding=encoding,
                     skiprows=1)  # Чтение файла в DataFrame, игнорируя первую строку

    # Выбор нужных столбцов, присвоение новых столбцов и удаление пустых строк
    selected_columns = df[['Номер скважины', 'Куст']].assign(
        number=df['Куст'].apply(lambda x: '7*' if pd.notna(x) else '8*'), bool='NO', slash='/').dropna(
        subset=['Номер скважины'])

    result_df = selected_columns[
        selected_columns['Номер скважины'] != 'WELSPECS']  # Удаление строк с заголовком "WELSPECS"

    result_df.to_csv("output_Welspecs.txt", sep='\t', index=False, header=False,
                     encoding='utf-8')  # Запись DataFrame в файл

    with open("output_Welspecs.txt", 'r', encoding='utf-8') as f:  # Открытие выходного файла для чтения
        lines = f.readlines()  # Чтение всех строк файла
        unique_lines = set(lines)  # Получение уникальных строк
        unique_data = ''.join(unique_lines).replace('\t\t',
                                                    '\t') + '/\n'  # Объединение уникальных строк и замена двойных табуляций

    with open("output_Welspecs.txt", 'w', encoding='utf-8') as f:  # Открытие выходного файла для записи
        f.write("WELSPECS\n")  # Запись заголовка "WELSPECS"
        f.write(unique_data)  # Запись уникальных данных


def process_compdatmd(perf_path):  # Функция для обработки данных compdatmd
    with open(perf_path, 'r', newline='', encoding='utf-8') as input_file:  # Открытие входного CSV-файла для чтения
        reader = csv.reader(input_file, delimiter=',')  # Создание объекта reader для чтения CSV-файла
        next(reader, None)  # Пропуск первой строки (заголовка)
        with open('output_Compdatmd.txt', 'w', encoding='utf-8') as output_file:  # Открытие выходного файла для записи
            output_file.write('COMPDATMD\n')  # Запись заголовка "COMPDATMD"
            for row in reader:  # Цикл по строкам CSV-файла
                name = row[0]  # Получение названия скважины
                md_start = row[7]  # Получение начальной глубины
                md_end = row[8]  # Получение конечной глубины
                open_shut = "SHUT" if row[-1] == "Нет" else "OPEN"  # Определение статуса "OPEN" или "SHUT"

                output_file.write(
                    f"'{name}'\t1*\t{md_start}\t{md_end}\t1*\t{open_shut}\t/\n")  # Запись строки данных в файл

            output_file.write(f"/\n")  # Запись разделителя в конце файла


def update_tp_csv():  # Функция для обновления файла TP.csv
    input_file_path = 'ТР.csv'  # Путь к входному файлу
    output_file_path = 'updatedTP.csv'  # Путь к выходному файлу

    df = pd.read_csv(input_file_path, skiprows=1)  # Чтение файла в DataFrame, игнорируя первую строку
    df['Дата'] = df['Дата'].fillna(method='ffill')  # Заполнение пропущенных значений в столбце "Дата" методом ffill

    # Словарь для замены сокращенных названий месяцев на английские названия
    month_translation = {'янв.': 'Jan', 'февр.': 'Feb', 'мар.': 'Mar', 'апр.': 'Apr', 'мая': 'May',
                         'июн.': 'Jun', 'июл.': 'Jul', 'авг.': 'Aug', 'сент.': 'Sep', 'окт.': 'Oct',
                         'нояб.': 'Nov', 'дек.': 'Dec'}
    df['Дата'] = df['Дата'].replace(month_translation, regex=True)
    df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce', format='%b.%Y').dt.strftime(
        '%d %b %Y')

    df = df.drop('Дата_temp', axis=1, errors='ignore')  # Удаление временного столбца "Дата_temp" (если существует)
    df.to_csv(output_file_path, index=False, encoding='utf-8')  # Запись DataFrame в файл


def process_wconhist():
    update_tp_csv()

    with open('updatedTP.csv', 'r', newline='',
              encoding='utf-8') as csvfile:  # Открытие обновленного CSV-файла для чтения
        reader = csv.reader(csvfile, delimiter=',')  # Создание объекта reader для чтения CSV-файла
        headers = next(reader, None)  # Получение заголовков столбцов
        date_column_index = headers.index('Дата')  # Получение индекса столбца "Дата"

        with open('output_Wconhist.txt', 'w', encoding='utf-8') as output_file:  # Открытие выходного файла для записи
            current_date = None  # Переменная для хранения текущей даты
            data_exists = False  # Флаг наличия данных
            data_buffer = []  # Буфер для хранения строк данных

            rows = list(reader)  # Получение всех строк из reader
            rows.reverse()  # Реверс строк (для обработки в обратном порядке)

            for row in rows:  # Цикл по строкам
                date_str = row[date_column_index]  # Получение значения даты из строки
                name = row[1]  # Получение названия скважины
                Rzab = row[16]  # Получение значения Rzab
                status = row[4]  # Получение статуса скважины
                if current_date is None:  # Если текущая дата не установлена
                    current_date = date_str  # Установка текущей даты
                    output_file.write("DATES\n")  # Запись заголовка "DATES"
                    output_file.write(f" {current_date} /\n/\n")  # Запись даты

                if date_str != current_date:  # Если дата изменилась
                    if data_exists:  # Если есть данные для предыдущей даты
                        output_file.write("WCONPROD\n")  # Запись заголовка "WCONPROD"
                        output_file.write('\n'.join(data_buffer))  # Запись данных из буфера
                        output_file.write('\n /\n')  # Запись разделителя
                    data_buffer = []  # Очистка буфера
                    current_date = date_str  # Обновление текущей даты
                    data_exists = False  # Сброс флага наличия данных
                    output_file.write("\nDATES\n")  # Запись заголовка "DATES"
                    output_file.write(f" {current_date} /\n/\n")  # Запись новой даты

                if Rzab != "":  # Если значение Rzab не пустое
                    data_exists = True  # Установка флага наличия данных
                    if status == "РАБ.":  # Если статус скважины "РАБ."
                        data_buffer.append(f" {name}\tOPEN\tBHP\t5*\t{Rzab}\t1*\t/")  # Добавление строки в буфер
                    else:
                        data_buffer.append(f" {name}\tSHUT\t/")  # Добавление строки в буфер

            if data_exists:  # Если есть данные для последней даты
                output_file.write("WCONPROD\n")  # Запись заголовка "WCONPROD"
                output_file.write('\n'.join(data_buffer))  # Запись данных из буфера
                output_file.write('\n/')  # Перевод строки

    with open('output_Wconhist.txt', 'r', encoding='utf-8') as f:  # Открытие выходного файла для чтения
        data = f.read().replace('\t\t', '\t').replace(',', '.')  # Замена двойных табуляций и запятых на точки

    with open('output_Wconhist.txt', 'w', encoding='utf-8') as f:  # Открытие выходного файла для записи
        f.write(data)  # Запись обработанных данных


def insert_or_replace(lines, index, content, word, nextword):
    replaced = False

    for i, line in enumerate(lines):
        if word in line:
            # Если слово найдено в строке, заменяем или вставляем контент
            existed_part_start = line.find(word)
            existed_part_end = line.find(nextword, existed_part_start)
            existed_part = line[existed_part_start:existed_part_end]
            lines[i] = line.replace(existed_part, content)
            replaced = True

    if not replaced:
        # Если слово не найдено ни в одной строке, вставляем контент на указанный индекс
        lines.insert(index, content)

    return lines


def merge_files():
    with open('Simulation_block\\Block\\Sch.inc', 'r', encoding='utf-8') as inc_file:
        lines = inc_file.readlines()

    with open('output_WellTracks.txt', 'r', encoding='utf-8') as welltracks:
        welltracks_content = welltracks.read()

    with open('output_Welspecs.txt', 'r', encoding='utf-8') as welspecs:
        welspecs_content = welspecs.read()

    with open('output_Compdatmd.txt', 'r', encoding='utf-8') as compdatmd:
        compdatmd_content = compdatmd.read()

    # Вставка или замена контента в исходном файле
    lines = insert_or_replace(lines, 0, '\n' + compdatmd_content, "COMPDATMD", "\n/")
    lines = insert_or_replace(lines, 1, '\n' + welspecs_content, "WELSPECS", "COMPDATMD")
    lines = insert_or_replace(lines, 2, welltracks_content, "WELLTRACK", "WELSPECS")

    with open('Simulation_block\\Block\\Sch.inc', 'w') as inc_file:  # Открытие исходного файла для
        inc_file.writelines(lines)  # Запись обновленных строк в файл


def update_wconhist():
    with open("output_Wconhist.txt", "r") as output_file:
        output = output_file.read()

    sections = output.split("DATES")  # Разделение содержимого файла на секции по слову "DATES"

    with open('Simulation_block\\Block\\Sch.inc', 'r') as inc_file:  # Открытие исходного файла для чтения
        lines = inc_file.readlines()  # Чтение всех строк файла

    for section in sections:  # Цикл по секциям
        if "WCONPROD" in section:  # Если секция содержит "WCONPROD"
            date_start = section.find("01 ")  # Получение индекса начала даты
            date_end = section.find("/", date_start)  # Получение индекса конца даты
            date = section[date_start:date_end].strip()  # Получение строки даты

            wconprod_from_output_start = section.find("WCONPROD")  # Получение индекса начала "WCONPROD" в секции
            wconprod_from_output_end = section.find("\n /",
                                                    wconprod_from_output_start)  # Получение индекса конца "WCONPROD" в секции
            wconprod_section_output = section[
                                      wconprod_from_output_start:wconprod_from_output_end + 3]  # Получение строки "WCONPROD" из секции

            schs_sections = ''.join(lines).split("DATES")  # Разделение исходного файла на секции по слову "DATES"

            if date in ''.join(lines):  # Если дата найдена в исходном файле
                for schs_section in schs_sections:  # Цикл по секциям исходного файла
                    if date in schs_section:  # Если дата найдена в секции
                        if "WCONPROD" in schs_section:  # Если секция содержит "WCONPROD"
                            wconprod_start = schs_section.find(
                                "WCONPROD")  # Получение индекса начала "WCONPROD" в секции
                            wconprod_end = schs_section.find("\n /",
                                                             wconprod_start)  # Получение индекса конца "WCONPROD" в секции
                            wconprod_section = schs_section[
                                               wconprod_start:wconprod_end + 3]  # Получение строки "WCONPROD" из секции
                            lines = ''.join(lines).replace(wconprod_section,
                                                           wconprod_section_output)  # Замена строки "WCONPROD" в исходном файле

                        else:  # Если секция не содержит "WCONPROD"
                            date_start = ''.join(lines).find(date)  # Получение индекса начала даты в исходном файле
                            date_end = ''.join(lines).find("/",
                                                           date_start)  # Получение индекса конца даты в исходном файле
                            lines = ''.join(lines)[:date_end + 4] + wconprod_section_output + ''.join(lines)[
                                                                                              date_end + 3:]  # Вставка строки "WCONPROD" в исходный файл
            else:  # Если дата не найдена в исходном файле
                lines += "DATES" + section  # Добавление секции в конец исходного файла

    # lines = lines.strip()  # Удаление начальных и конечных пробелов из строки (Не помню зачем сделано, ломает код)

    with open("Simulation_block\\Block\\Sch.inc", "w") as file2:  # Открытие исходного файла для записи
        file2.writelines(lines)  # Запись обновленных строк в файл

    subprocess.run([f'Simulation_block\\Block.bat', "632"])


process_welltrack(df_inclin, df_trajectory)
process_welspecs(tp_path)
process_compdatmd(perf_path)
process_wconhist()
merge_files()
update_wconhist()
