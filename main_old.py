import subprocess
from datetime import datetime

import pandas as pd
import chardet
import csv
import locale

from babel.dates import format_date

# Часть 1 WELLTRACK

df_inclin = pd.read_csv('2Инклинометрия_НП4.csv')  # Инклинометрия_НП4
df_trajectory = pd.read_csv('2Траектория_НП4.csv')  # Траектория_НП4

merged_df = pd.concat([df_inclin, df_trajectory], axis=1)

with open('output1.txt', 'w', encoding='utf-8') as file:
    prev_well = None
    for index, row in merged_df.iterrows():
        well = row['Скважина'].iloc[0]
        data = row.drop('Скважина')

        if prev_well is None or well != prev_well:
            if prev_well is not None:
                file.seek(file.tell() - 2)
                file.write('\t/\n/\n')

            file.write(f'WELLTRACK "{well}"\n')
            prev_well = well

        row = f'{row["X"]}\t{row["Y"]}\t{row["Zабс"]}\t{row["Глубина"]}'.replace(",", ".")

        file.write(row + '\n')

    file.seek(file.tell() - 2)
    file.write('\t/\n')
    file.write('/')

# Часть 2   WELSPECS            Ввод - TP.csv, Вывод в output2.txt
input_file_path = 'ТР.csv'
output_file_path = 'output2.txt'

with open(input_file_path, 'rb') as f:
    encoding = chardet.detect(f.read())['encoding']

df = pd.read_csv(input_file_path, sep=',', encoding=encoding, skiprows=1)
selected_columns = df[['Номер скважины', 'Куст']].assign(
    number=df['Куст'].apply(lambda x: '7*' if pd.notna(x) else '8*'), bool='NO', slash='/').dropna(
    subset=['Номер скважины'])

result_df = selected_columns[selected_columns['Номер скважины'] != 'WELSPECS']

result_df.to_csv(output_file_path, sep='\t', index=False, header=False, encoding='utf-8')

with open(output_file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()
    unique_lines = set(lines)
    unique_data = ''.join(unique_lines).replace('\t\t', '\t') + '/\n'

with open(output_file_path, 'w', encoding='utf-8') as f:
    f.write("WELSPECS\n")
    f.write(unique_data)

# Часть 3  COMPDATMD            Ввод - Perf_NP4.csv, Вывод в output3.txt
with open('2Perf_NP4.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    next(reader, None)
    with open('output3.txt', 'w', encoding='utf-8') as outputfile:
        outputfile.write('COMPDATMD\n')
        for row in reader:
            name = row[0]
            md_start = row[7]
            md_end = row[8]
            open_shut = "SHUT" if row[-1] == "Нет" else "OPEN"

            outputfile.write(f"'{name}'\t1*\t{md_start}\t{md_end}\t1*\t{open_shut}\t/\n")

        outputfile.write(f"/\n")

# Часть 4 WCONHIST               Ввод - TP.csv, Вывод в output4.txt

locale.setlocale(locale.LC_TIME, 'ru_RU.utf-8')
locale.setlocale(locale.LC_TIME, 'en_EN.utf-8')

import pandas as pd


def updateTPcsv():
    input_file_path = 'ТР.csv'
    output_file_path = 'updatedTP.csv'

    df = pd.read_csv(input_file_path, skiprows=1)
    df['Дата'] = df['Дата'].fillna(method='ffill')
    month_translation = {'янв.': 'Jan', 'февр.': 'Feb', 'мар.': 'Mar', 'апр.': 'Apr', 'мая': 'May',
                         'июн.': 'Jun', 'июл.': 'Jul', 'авг.': 'Aug', 'сент.': 'Sep', 'окт.': 'Oct',
                         'нояб.': 'Nov', 'дек.': 'Dec'}
    df['Дата'] = df['Дата'].replace(month_translation, regex=True)
    df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce', format='%b.%Y').dt.strftime(
        '%d %b %Y')

    df = df.drop('Дата_temp', axis=1, errors='ignore')
    df.to_csv(output_file_path, index=False, encoding='utf-8')


def reformat_date(current_data):  # Форматирование даты к формату - {мес..год}
    data = datetime.strptime(current_data, "%m.%Y")
    short_month = format_date(data, format='MMM', locale='ru').lower()
    result = f"{short_month}.{data.year}"
    return result


def extract_q_stolb(file_path, q_stolb_column, target_name, current_data):  # Извлечение Глубины из файла ТР
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=',')
        headers = next(reader, None)
        formated_data = reformat_date(current_data)
        q_stolb_index = headers.index(q_stolb_column)
        for row in reader:
            well_name = row[1]
            if well_name == target_name and formated_data == row[3]:
                return row[q_stolb_index]


updateTPcsv()
with open('МЭР_НП4.csv', 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile, delimiter=',')
    headers = next(reader, None)
    date_column_index = headers.index('Дата')

    with open('output4.txt', 'w', encoding='utf-8') as outputfile:

        outputfile.write('DATES\n')
        current_data = next(reader, None)[date_column_index]
        date_obj = datetime.strptime(current_data, '%m.%Y')
        outputfile.write(f" 01 {date_obj.strftime('%B')[:3]} {date_obj.year} /\n/\n")
        outputfile.write("WCONHIST\n")
        rows = list(reader)
        for row in rows[:-1]:  # Убрал последнюю строчку из МЭР из-за некорректности данных
            date_str = row[date_column_index]
            name = row[1]
            oil_rate = float(row[20].replace(",", ".")) / 0.85
            water_rate = float(row[21].replace(",", ".")) / 0.1005
            gas_rate = float(row[-1].replace(",", "."))
            q_stolb = extract_q_stolb('updatedTP.csv', "Р(заб)", name, current_data)
            if date_str != current_data:
                outputfile.write("/\n\n")
                current_data = date_str
                date_obj = datetime.strptime(date_str, '%m.%Y')
                outputfile.write('DATES\n')
                outputfile.write(f" 01 {date_obj.strftime('%B')[:3]} {date_obj.year} /\n/\n")
                outputfile.write("\nWCONHIST\n")
            if current_data == row[date_column_index] and int(oil_rate) + int(water_rate) != 0 and q_stolb is not None:
                outputfile.write(f"{name}\tOPEN\tBHP\t{oil_rate}\t{water_rate}\t{gas_rate}\t3*\t{q_stolb}\t/\n")
            elif current_data == row[date_column_index] and int(oil_rate) + int(water_rate) != 0:
                outputfile.write(f"{name}\tOPEN\tLRAT\t{oil_rate}\t{water_rate}\t{gas_rate}\t/\n")

with open('output4.txt', 'r', encoding='utf-8') as f:
    data = f.read().replace('\t\t', '\t') + '/\n'
with open('output4.txt', 'w', encoding='utf-8') as f:
    f.write(data)

# Вставка в FULL_TNAV_SCH.INC

with open('C:\\Simulation_block\\Block\\Sch.inc', 'r') as inc_file:
    lines = inc_file.readlines()

with open('output1.txt', 'r') as welltracks:
    welltracks_content = welltracks.read()

with open('output2.txt', 'r') as welspecs:
    welspecs_content = welspecs.read()

with open('output3.txt', 'r') as compdatmd:
    compdatmd_content = compdatmd.read()

# Вставка первых трех файлов
with open('schs.txt', 'r') as inc_file:
    lines = inc_file.read()


def insert_or_replace(lines, index, content, word, nextword):
    if word in lines:
        existed_part_start = lines.find(word)
        existed_part_end = lines.find(nextword, existed_part_start)
        existed_part = lines[existed_part_start:existed_part_end]
        lines.replace(existed_part, content)
    else:
        lines = lines[:index] + content + '\n' + lines[index:]

    return lines


lines = insert_or_replace(lines, 0, '\n' + compdatmd_content, "COMPDATMD", "\n/")
lines = insert_or_replace(lines, 1, '\n' + welspecs_content, "WELSPECS", "COMPDATMD")
lines = insert_or_replace(lines, 2, welltracks_content, "WELLTRACK", "WELSPECS")

with open('schs.txt', 'w') as inc_file:
    inc_file.write(lines)

with open("output4.txt", "r") as file1:
    data_file1 = file1.read()

sections = data_file1.split("DATES")

for section in sections:
    if "WCONHIST" in section:
        date_start = section.find("01 ")
        date_end = section.find("/", date_start)
        date = section[date_start:date_end].strip()

        wconprod_from_output_start = section.find("WCONHIST")
        wconprod_from_output_end = section.find("\n /", wconprod_from_output_start)
        wconprod_section_output = section[wconprod_from_output_start:wconprod_from_output_end + 3]
        schs_sections = lines.split("DATES")

        if date in lines:
            for schs_section in schs_sections:
                if date in schs_section:
                    if "WCONHIST" in schs_section:
                        wconprod_start = schs_section.find("WCONHIST")
                        wconprod_end = schs_section.find("\n /", wconprod_start)
                        wconprod_section = schs_section[wconprod_start:wconprod_end + 3]
                        lines = lines.replace(wconprod_section, wconprod_section_output)

                    else:
                        date_start = lines.find(date)
                        date_end = lines.find("/", date_start)
                        lines = lines[:date_end + 4] + wconprod_section_output + lines[date_end + 3:]

        else:
            lines += "DATES" + section

lines = lines.strip()

with open("schs.txt", "w") as file2:
    file2.write(lines)

with open("schs.txt", "w") as inc_file:
    inc_file.writelines(lines)

subprocess.run([f'C:\\Simulation_block\\Block.bat', "632"])
