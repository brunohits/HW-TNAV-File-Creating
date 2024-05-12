import subprocess
import pandas as pd
import chardet
import csv
import locale
from datetime import datetime
from babel.dates import format_date


def process_welltrack():
    df_inclin = pd.read_csv('Инклинометрия_НП4.csv')
    df_trajectory = pd.read_csv('Траектория_НП4.csv')

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

            file.write(f'{row["X"]}\t{row["Y"]}\t{row["Zабс"]}\t{row["Глубина"]}\n')

        file.seek(file.tell() - 2)
        file.write('\t/\n')
        file.write('/')


def process_welspecs():
    input_file_path = 'ТР.csv'
    output_file_path = 'output2.txt'
    with open(input_file_path, 'rb') as f:
        encoding = chardet.detect(f.read())['encoding']
    df = pd.read_csv(input_file_path, sep=',', encoding=encoding, skiprows=1)
    selected_columns = df[['Номер скважины', 'Куст']].assign(
        number=df['Куст'].apply(lambda x: '7*' if pd.notna(x) else '8*'), bool='NO', slash='/').dropna(
        subset=['Номер скважины'])
    wellspecs_df = pd.DataFrame([['WELSPECS', '', '', '', '']], columns=selected_columns.columns)
    result_df = pd.concat([wellspecs_df, selected_columns], ignore_index=True)
    result_df.to_csv(output_file_path, sep='\t', index=False, header=False, encoding='utf-8')
    with open(output_file_path, 'r', encoding='utf-8') as f:
        data = f.read().replace('\t\t', '\t') + '/\n'
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(data)


def process_compdatmd():
    with open('Perf_NP4.csv', 'r', newline='', encoding='utf-8') as csvfile:
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


def reformat_date(current_data):
    data = datetime.strptime(current_data, "%m.%Y")
    short_month = format_date(data, format='MMM', locale='ru').lower()
    result = f"{short_month}.{data.year}"
    return result


def extract_q_stolb(file_path, q_stolb_column, target_name, current_data):
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=',')
        headers = next(reader, None)
        formated_data = reformat_date(current_data)
        q_stolb_index = headers.index(q_stolb_column)
        for row in reader:
            well_name = row[1]
            if well_name == target_name and formated_data == row[3]:
                return row[q_stolb_index]


def process_wconhist():
    updateTPcsv()
    with open('МЭР_НП4.csv', 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        headers = next(reader, None)
        date_column_index = headers.index('Дата')

        with open('output4.txt', 'w', encoding='utf-8') as outputfile:
            outputfile.write('DATES\n')
            current_data = next(reader, None)[date_column_index]
            date_obj = datetime.strptime(current_data, '%m.%Y')
            outputfile.write(f"1 {date_obj.strftime('%B').upper()} {date_obj.year} /\n/\n")
            outputfile.write("WCONHIST\n")
            rows = list(reader)
            for row in rows[:-1]:
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
                    outputfile.write(f"  1 {date_obj.strftime('%B').upper()} {date_obj.year} /\n  /\n")
                    outputfile.write("\nWCONHIST\n")
                if current_data == row[date_column_index] and int(oil_rate) + int(
                        water_rate) != 0 and q_stolb is not None:
                    outputfile.write(f"{name}\tOPEN\tBHP\t{oil_rate}\t{water_rate}\t{gas_rate}\t3*\t{q_stolb}\t/\n")
                elif current_data == row[date_column_index] and int(oil_rate) + int(water_rate) != 0:
                    outputfile.write(f"{name}\tOPEN\tLRAT\t{oil_rate}\t{water_rate}\t{gas_rate}\t/\n")

    with open('output4.txt', 'r', encoding='utf-8') as f:
        data = f.read().replace('\t\t', '\t') + '/\n'
    with open('output4.txt', 'w', encoding='utf-8') as f:
        f.write(data)


def updateTPcsv():
    input_file_path = 'ТР.csv'
    output_file_path = 'updatedTP.csv'
    df = pd.read_csv(input_file_path, skiprows=1)
    date_column = "Дата"
    if date_column in df.columns:
        df[date_column] = df[date_column].fillna(method='ffill')
        df.to_csv(output_file_path, index=False)


def merge_files():
    with open('C:\\Simulation_block\\Block\FULL_TNAV__632_SCH.INC', 'r') as inc_file:
        lines = inc_file.readlines()
    endskip_index = lines.index('ENDSKIP                                -- Generated : Petrel\n')
    with open('output1.txt', 'r') as welltracks:
        welltracks_content = welltracks.read()
        lines.insert(endskip_index + 1, '\n' + welltracks_content + '\n')

    with open('output2.txt', 'r') as welspecs:
        welspecs_content = welspecs.read()
    lines.insert(endskip_index + 2, '\n' + welspecs_content)

    with open('output3.txt', 'r') as compdatmd:
        compdatmd_content = compdatmd.read()
    lines.insert(endskip_index + 3, '\n' + compdatmd_content)

    with open('output4.txt', 'r') as wconhist:
        wconhist_content = wconhist.read()
    lines.insert(endskip_index + 4, '\n' + wconhist_content)

    with open('C:\\Simulation_block\\Block\\FULL_TNAV__632_SCH.INC', 'w') as inc_file:
        inc_file.writelines(lines)

    subprocess.run([f'C:\\Simulation_block\\Block.bat', "632"])


process_welltrack()
process_welspecs()
process_compdatmd()
process_wconhist()
merge_files()
