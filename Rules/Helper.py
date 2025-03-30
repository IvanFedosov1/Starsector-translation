import csv
import json
import re
import os
from typing import List, Dict, Any, Optional

# --- НАСТРОЙКИ ---
# Режим работы: 'csv2json' или 'json2csv'
MODE: str = 'json2csv' # ИЗМЕНИТЕ ЭТО

# Пути к файлам (ИЗМЕНИТЕ ЭТО)
CSV_INPUT_FILE: str = 'rules.csv'       # Входной CSV для csv2json
JSON_FILE: str = 'rules_for_translation.json' # Выходной JSON для csv2json / Входной JSON для json2csv
CSV_OUTPUT_FILE: str = 'translated_rules.csv' # Выходной CSV для json2csv
ORIGINAL_CSV_FILE: str = 'rules.csv'    # Оригинальный CSV (НУЖЕН для json2csv для восстановления структуры)

# Кодировки файлов (ИЗМЕНИТЕ ЭТО, если нужно)
# Попробуйте 'cp1251' если 'utf-8' не сработал. Другие варианты: 'cp1252', 'latin-1'
CSV_ENCODING: str = 'cp1251' # !!! ИЗМЕНЕНО НА CP1251 - ПОПРОБУЙТЕ ЭТО !!!
JSON_ENCODING: str = 'utf-8'      # Обычно всегда utf-8
OUTPUT_CSV_ENCODING: str = 'utf-8' # Рекомендуется utf-8 для выходного CSV
# ------------------

# Регулярное выражение для разбора опций: Priority(opt):ID:Text
OPTION_PATTERN_PRIO = re.compile(r"^(?P<priority>\d+):(?P<id>[^:]+):(?P<text>.*)$")
# Регулярное выражение для разбора опций: ID:Text
OPTION_PATTERN_NO_PRIO = re.compile(r"^(?P<id>[^:]+):(?P<text>.*)$")

def parse_options_string(options_str: str) -> List[Dict[str, Any]]:
    """Разбирает многострочное поле 'options' в список объектов."""
    parsed_options = []
    if not options_str.strip():
        return parsed_options

    lines = options_str.split('\n')
    for line in lines:
        line_strip = line.strip()
        if not line_strip:
            continue # Пропускаем пустые строки

        # Явно обрабатываем строки, начинающиеся с # как комментарии (raw)
        if line_strip.startswith('#'):
            print(f"  Info: Storing commented option line as raw: '{line_strip}'")
            parsed_options.append({"raw": line_strip})
            continue

        match_prio = OPTION_PATTERN_PRIO.match(line_strip)
        if match_prio:
            parsed_options.append(match_prio.groupdict())
            continue

        match_no_prio = OPTION_PATTERN_NO_PRIO.match(line_strip)
        if match_no_prio:
            parsed_options.append(match_no_prio.groupdict())
            continue

        # Если ни один паттерн не подошел, сохраняем как 'raw'
        print(f"  Warning: Could not parse option line: '{line_strip}'. Storing as raw.")
        parsed_options.append({"raw": line_strip})

    return parsed_options

def build_options_string(options_list: List[Dict[str, Any]]) -> str:
    """Собирает список объектов опций обратно в многострочную строку."""
    lines = []
    for option_data in options_list:
        if "raw" in option_data:
            # Проверяем, не пустая ли raw строка (если вдруг сохранили пустые)
            if option_data["raw"] or isinstance(option_data["raw"], str) :
                 lines.append(str(option_data["raw"]))
        elif "priority" in option_data and "id" in option_data and "text" in option_data:
            lines.append(f"{option_data['priority']}:{option_data['id']}:{option_data['text']}")
        elif "id" in option_data and "text" in option_data:
             # Проверяем, что priority точно нет, чтобы не дублировать логику
             if "priority" not in option_data:
                 lines.append(f"{option_data['id']}:{option_data['text']}")
        # else: пропускаем непонятные объекты или объекты без текста
            # print(f"Skipping unknown/incomplete option data: {option_data}")

    return '\n'.join(lines)

def csv_to_json(csv_filepath, json_filepath):
    """Конвертирует CSV в JSON, сохраняя структуру строк."""
    json_data = []
    row_num = 1 # Начинаем с 1 для заголовка
    print(f"Reading CSV: {csv_filepath} with encoding {CSV_ENCODING}")
    try:
        # Добавляем errors='ignore' или errors='replace' если смена кодировки не помогает
        # но это может привести к потере данных. Лучше найти правильную кодировку.
        with open(csv_filepath, 'r', encoding=CSV_ENCODING, newline='' #, errors='ignore' # <-- крайняя мера
                 ) as csvfile:
            reader = csv.reader(csvfile)
            try:
                header = next(reader)
                print(f"CSV Header: {header}")
                row_num += 1
            except StopIteration:
                print("Error: CSV file is empty or has no header.")
                return

            expected_columns = 7 # Ожидаемое количество колонок из вашего примера
            if len(header) != expected_columns:
                 print(f"Warning: Expected {expected_columns} columns in header, found {len(header)}. Proceeding anyway.")

            for i, row in enumerate(reader):
                current_row_processing = row_num + i
                if not any(field.strip() for field in row):
                    continue

                if row and row[0].strip().startswith('#'):
                     json_data.append({
                         "_row_number": current_row_processing,
                         "_type": "comment",
                         "comment_line": ','.join(row)
                     })
                     continue

                # Убедимся, что у нас правильное количество столбцов, дополним пустыми если нужно
                if len(row) < expected_columns:
                    print(f"Warning: Row {current_row_processing} has {len(row)} fields, expected {expected_columns}. Padding with empty strings.")
                    row.extend([''] * (expected_columns - len(row)))
                elif len(row) > expected_columns:
                     print(f"Warning: Row {current_row_processing} has {len(row)} fields, expected {expected_columns}. Truncating.")
                     row = row[:expected_columns]

                row_dict = {"_row_number": current_row_processing, "_type": "data"}
                is_empty_data_row = True
                # Используем стандартные имена, если заголовок был некорректный
                effective_header = header if len(header) == expected_columns else ['id', 'trigger', 'conditions', 'script', 'text', 'options', 'notes']

                for idx, field_name in enumerate(effective_header):
                    value = row[idx]
                    if field_name == 'options':
                        row_dict[field_name] = parse_options_string(value)
                    else:
                        row_dict[field_name] = value

                    if isinstance(value, str) and value.strip():
                        is_empty_data_row = False
                    elif field_name == 'options' and row_dict[field_name]: # Если список опций не пуст
                        is_empty_data_row = False


                if is_empty_data_row:
                    continue

                json_data.append(row_dict)

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        return
    except UnicodeDecodeError as e:
        print(f"\n!!! Error: Failed to decode CSV file using encoding '{CSV_ENCODING}'. !!!")
        print(f"!!! Please check the CSV_ENCODING setting in the script. Common alternatives are 'utf-8', 'cp1252', 'latin-1'. !!!")
        print(f"Specific error: {e}\n")
        return
    except Exception as e:
        print(f"Error reading CSV file (around row {current_row_processing}): {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"Writing JSON: {json_filepath} with encoding {JSON_ENCODING}")
    try:
        with open(json_filepath, 'w', encoding=JSON_ENCODING) as jsonfile:
            json.dump(json_data, jsonfile, ensure_ascii=False, indent=2)
        print(f"CSV to JSON conversion successful. {len(json_data)} objects written.")
    except Exception as e:
        print(f"Error writing JSON file: {e}")

def json_to_csv(json_filepath, original_csv_filepath, output_csv_filepath):
    """Собирает новый CSV из JSON данных и оригинального CSV для комментариев/структуры."""
    print(f"Reading translations JSON: {json_filepath} with encoding {JSON_ENCODING}")
    try:
        with open(json_filepath, 'r', encoding=JSON_ENCODING) as jsonfile:
            json_data_list = json.load(jsonfile)
            json_data_map = {item['_row_number']: item for item in json_data_list if '_row_number' in item}
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_filepath}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
        return
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    print(f"Reading original CSV: {original_csv_filepath} with encoding {CSV_ENCODING}")
    print(f"Writing output CSV: {output_csv_filepath} with encoding {OUTPUT_CSV_ENCODING}")

    # Переменная для отслеживания строки при ошибке
    current_row_num = 1
    try:
        with open(original_csv_filepath, 'r', encoding=CSV_ENCODING, newline='') as infile, \
             open(output_csv_filepath, 'w', encoding=OUTPUT_CSV_ENCODING, newline='') as outfile:

            reader = csv.reader(infile)
            # Определяем диалект записи (кавычки и т.д.)
            # QUOTE_MINIMAL старается не ставить кавычки, если не нужно (как в оригинале)
            # QUOTE_ALL ставит кавычки вокруг всех полей (надежнее для перевода)
            # QUOTE_NONNUMERIC ставит кавычки вокруг нечисловых полей
            writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL) # Попробуйте QUOTE_ALL, если QUOTE_MINIMAL вызывает проблемы

            try:
                header = next(reader)
                writer.writerow(header) # Записываем заголовок
                current_row_num += 1
            except StopIteration:
                 print("Error: Original CSV file is empty or has no header.")
                 return

            expected_columns = 7
            effective_header = header if len(header) == expected_columns else ['id', 'trigger', 'conditions', 'script', 'text', 'options', 'notes']


            for original_row in reader:
                translated_data = json_data_map.get(current_row_num)

                if translated_data:
                    row_type = translated_data.get("_type")
                    if row_type == "comment":
                        # csv.reader не может читать строку, уже разделенную запятыми
                        # Проще всего записать строку "как есть"
                        outfile.write(translated_data.get("comment_line","") + '\n')
                    elif row_type == "malformed_row":
                         writer.writerow(translated_data.get("raw_data", original_row))
                    elif row_type == "data":
                        output_row = []
                        for field_name in effective_header:
                            if field_name == 'options':
                                options_list = translated_data.get(field_name, [])
                                output_row.append(build_options_string(options_list))
                            else:
                                # Получаем значение из JSON, если оно там есть, иначе оставляем пустым
                                output_row.append(translated_data.get(field_name, ''))
                        # Убедимся что количество колонок совпадает с заголовком
                        if len(output_row) < len(effective_header):
                             output_row.extend([''] * (len(effective_header) - len(output_row)))
                        elif len(output_row) > len(effective_header):
                             output_row = output_row[:len(effective_header)]
                        writer.writerow(output_row)

                else:
                     # Записываем оригинальную строку, если для нее нет данных в JSON
                     writer.writerow(original_row)

                current_row_num += 1

        print("JSON to CSV conversion successful.")

    except FileNotFoundError:
        print(f"Error: Original CSV file not found at {original_csv_filepath}")
    except UnicodeDecodeError as e:
        print(f"\n!!! Error: Failed to decode Original CSV file using encoding '{CSV_ENCODING}' during json2csv. !!!")
        print(f"!!! Please check the CSV_ENCODING setting in the script. !!!")
        print(f"Specific error: {e}\n")
        return
    except Exception as e:
        print(f"Error during JSON to CSV conversion (around row {current_row_num}): {e}")
        import traceback
        traceback.print_exc()

# --- Основной блок ---
if __name__ == "__main__":
    if MODE == 'csv2json':
        print(f"Starting CSV to JSON conversion...")
        print(f"  Input CSV: {CSV_INPUT_FILE}")
        print(f"  Output JSON: {JSON_FILE}")
        csv_to_json(CSV_INPUT_FILE, JSON_FILE)
    elif MODE == 'json2csv':
        print(f"Starting JSON to CSV conversion...")
        print(f"  Input JSON: {JSON_FILE}")
        print(f"  Original CSV: {ORIGINAL_CSV_FILE}")
        print(f"  Output CSV: {CSV_OUTPUT_FILE}")
        if not os.path.exists(ORIGINAL_CSV_FILE):
             print(f"Error: Original CSV file '{ORIGINAL_CSV_FILE}' is required for json2csv mode and was not found.")
        else:
             json_to_csv(JSON_FILE, ORIGINAL_CSV_FILE, CSV_OUTPUT_FILE)
    else:
        print(f"Error: Unknown MODE '{MODE}'. Please set MODE to 'csv2json' or 'json2csv'.")

    print("Script finished.")