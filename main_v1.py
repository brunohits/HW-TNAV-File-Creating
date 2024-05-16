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


def generate_welltrack(df_inclin, df_trajectory):  # Функция для генерации welltrack

    merged_df = pd.concat([df_inclin, df_trajectory], axis=1)  # Объединение данных инклинометрии и траектории

    with open('output_WellTracks.txt', 'w', encoding='utf-8') as file:
        prev_well = None  # Переменная для хранения предыдущего названия скважины
        for index, row in merged_df.iterrows():
            well = row['Скважина'].iloc[0]  # Получение названия скважины
            data = row.drop('Скважина')  # Удаление столбца "Скважина" из строки

            if prev_well is None or well != prev_well:
                if prev_well is not None:
                    file.seek(file.tell() - 2)
                    file.write('\t/\n/\n')  # Запись разделителя между скважинами

                file.write(f'WELLTRACK "{well}"\n')  # Запись названия скважины в файл
                prev_well = well  # Обновление предыдущей скважины

            row = f'{row["X"]}\t{row["Y"]}\t{row["Zабс"]}\t{row["Глубина"]}'.replace(",",
                                                                                     ".")  # Форматирование строки данных

            file.write(row + '\n')

        file.seek(file.tell() - 2)
        file.write('\t/\n')
        file.write('/')


def generate_welspecs(tp_path):  # Функция для генерации welspecs
    # Определение кодировки файла
    with open(tp_path, 'rb') as f:
        encoding = chardet.detect(f.read())['encoding']

    # Чтение файла в DataFrame
    df = pd.read_csv(tp_path, sep=',', encoding=encoding, skiprows=1)

    # Выбор нужных столбцов
    selected_columns = df[['Номер скважины', 'Куст']].assign(
        number=df['Куст'].apply(lambda x: '7*' if pd.notna(x) else '8*'), bool='NO', slash='/').dropna(
        subset=['Номер скважины'])

    # Запись DataFrame в файл
    result_df = selected_columns[selected_columns['Номер скважины'] != 'WELSPECS']
    result_df.to_csv("output_Welspecs.txt", sep='\t', index=False, header=False, encoding='utf-8')

    # Обработка уникальных строк
    with open("output_Welspecs.txt", 'r', encoding='utf-8') as f:
        lines = f.readlines()
        unique_lines = set(lines)
        unique_data = ''.join(unique_lines).replace('\t\t', '\t') + '/\n'

    with open("output_Welspecs.txt", 'w', encoding='utf-8') as f:
        f.write("WELSPECS\n")
        f.write(unique_data)


# Функция для генерации compdatmd
def generate_compdatmd(perf_path):
    # Чтение входного CSV-файла и запись в выходной файл
    with open(perf_path, 'r', newline='', encoding='utf-8') as input_file:
        reader = csv.reader(input_file, delimiter=',')
        next(reader, None)  # Пропуск заголовка
        with open('output_Compdatmd.txt', 'w', encoding='utf-8') as output_file:
            output_file.write('COMPDATMD\n')  # Запись заголовка "COMPDATMD"
            for row in reader:
                # Извлечение данных из строки CSV и запись в файл
                name = row[0]
                md_start = row[7]
                md_end = row[8]
                open_shut = "SHUT" if row[-1] == "Нет" else "OPEN"
                output_file.write(f"'{name}'\t1*\t{md_start}\t{md_end}\t1*\t{open_shut}\t/\n")
            output_file.write(f"/\n")  # Запись разделителя в конце файла


def update_tp_csv(tp_path):  # Функция для обновления файла TP.csv
    output_file_path = 'updatedTP.csv'  # Путь к выходному файлу

    df = pd.read_csv(tp_path, skiprows=1)  # Чтение файла в DataFrame, игнорируя первую строку
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


def generate_wconhist():
    locale.setlocale(locale.LC_TIME, 'ru_RU.utf-8')
    locale.setlocale(locale.LC_TIME, 'en_EN.utf-8')

    update_tp_csv(tp_path)

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
    if word in lines:
        existed_part_start = lines.find(word)
        existed_part_end = lines.find(nextword, existed_part_start)
        existed_part = lines[existed_part_start:existed_part_end]
        lines = lines.replace(existed_part, content)
    else:
        lines = lines[:index] + content + '\n' + lines[index:]

    return lines


def update_wconprod_in_sch_inc():
    # Чтение содержимого сгенерированных файлов
    with open('output_WellTracks.txt', 'r') as welltracks:
        welltracks_content = welltracks.read()

    with open('output_Welspecs.txt', 'r') as welspecs:
        welspecs_content = welspecs.read()

    with open('output_Compdatmd.txt', 'r') as compdatmd:
        compdatmd_content = compdatmd.read()

    # Чтение содержимого файла Sch.inc
    with open('Simulation_block\\Block\\Sch.inc', 'r') as inc_file:
        lines = inc_file.read()

    # Вставка или замена данных в Sch.inc
    lines = insert_or_replace(lines, 0, '\n' + compdatmd_content, "COMPDATMD", "\n/")
    lines = insert_or_replace(lines, 1, '\n' + welspecs_content, "WELSPECS", "COMPDATMD")
    lines = insert_or_replace(lines, 2, welltracks_content, "WELLTRACK", "WELSPECS")

    # Запись обновленного содержимого Sch.inc
    with open('Simulation_block\\Block\\Sch.inc', 'w') as inc_file:
        inc_file.write(lines)

    # Чтение данных из файла output_Wconhist.txt
    with open("output_Wconhist.txt", "r") as file1:
        data_file1 = file1.read()

    # Разделение данных на секции по датам
    sections = data_file1.split("DATES")

    # Обновление данных WCONPROD в Sch.inc
    for section in sections:
        if "WCONPROD" in section:
            # Нахождение начальной и конечной дат для текущей секции
            date_start = section.find("01 ")
            date_end = section.find("/", date_start)
            date = section[date_start:date_end].strip()

            # Нахождение данных WCONPROD для текущей секции
            wconprod_from_output_start = section.find("WCONPROD")
            wconprod_from_output_end = section.find("\n /", wconprod_from_output_start)
            wconprod_section_output = section[wconprod_from_output_start:wconprod_from_output_end + 3]

            # Разделение содержимого Sch.inc на секции по датам
            schs_sections = lines.split("DATES")

            # Проверка наличия текущей даты в Sch.inc и обновление данных WCONPROD
            if date in lines:
                for schs_section in schs_sections:
                    if date in schs_section:
                        if "WCONPROD" in schs_section:
                            wconprod_start = schs_section.find("WCONPROD")
                            wconprod_end = schs_section.find("\n /", wconprod_start)
                            wconprod_section = schs_section[wconprod_start:wconprod_end + 3]
                            lines = lines.replace(wconprod_section, wconprod_section_output)
                        else:
                            date_start = lines.find(date)
                            date_end = lines.find("/", date_start)
                            lines = lines[:date_end + 4] + wconprod_section_output + lines[date_end + 3:]
            else:
                lines += "DATES" + section

    # Удаление лишних пробельных символов
    lines = lines.strip()

    with open('Simulation_block\\Block\\Sch.inc', 'w') as inc_file:
        inc_file.writelines(lines)


generate_welltrack(df_inclin, df_trajectory)
generate_welspecs(tp_path)
generate_compdatmd(perf_path)
generate_wconhist()
update_wconprod_in_sch_inc()

subprocess.run([f'Simulation_block\\Block.bat', "632"])
