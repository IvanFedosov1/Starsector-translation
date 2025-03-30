import csv
import json
import re
import os
from typing import List, Dict, Any, Optional

# --- НАСТРОЙКИ ---
# Режим работы: 'csv2json' или 'json2csv'
MODE: str = 'json2csv' # ИЗМЕНИТЕ ЭТО

# Пути к файлам (ИЗМЕНИТЕ ЭТО)
CSV_INPUT_FILE: str = 'rules.csv'
JSON_FILE: str = 'rules_for_translation.json'
CSV_OUTPUT_FILE: str = 'translated_rules.csv'
# ORIGINAL_CSV_FILE больше не нужен напрямую для json2csv

# Кодировки файлов (ИЗМЕНИТЕ ЭТО, если нужно)
CSV_ENCODING: str = 'cp1251'
JSON_ENCODING: str = 'utf-8'
OUTPUT_CSV_ENCODING: str = 'utf-8'
# ------------------
# Регулярное выражение для разбора опций: Priority(opt):ID:Text
OPTION_PATTERN_PRIO = re.compile(r"^(?P<priority>\d+):(?P<id>[^:]+):(?P<text>.*)$")
# Регулярное выражение для разбора опций: ID:Text
OPTION_PATTERN_NO_PRIO = re.compile(r"^(?P<id>[^:]+):(?P<text>.*)$")
# ----------------------------------------


# ... (Функции parse_options_string и build_options_string остаются как в предыдущей версии) ...
def parse_options_string(options_str: str) -> List[Dict[str, Any]]:
    """Разбирает многострочное поле 'options' в список объектов, сохраняя whitespace."""
    parsed_options = []
    if not options_str: # Обработка полностью пустой строки
        return parsed_options

    lines = options_str.split('\n')
    for line in lines:
        if line.strip() == "":
            parsed_options.append({"raw": line})
            continue

        match_prio = OPTION_PATTERN_PRIO.match(line)
        if match_prio:
            parsed_options.append(match_prio.groupdict())
            continue

        match_no_prio = OPTION_PATTERN_NO_PRIO.match(line)
        if match_no_prio:
            parsed_options.append(match_no_prio.groupdict())
            continue

        parsed_options.append({"raw": line})

    return parsed_options

def build_options_string(options_list: List[Dict[str, Any]]) -> str:
    """Собирает список объектов опций обратно в многострочную строку."""
    lines = []
    for option_data in options_list:
        if "raw" in option_data:
             if option_data["raw"] or isinstance(option_data["raw"], str):
                 lines.append(str(option_data["raw"]))
        elif "priority" in option_data and "id" in option_data and "text" in option_data:
            lines.append(f"{option_data['priority']}:{option_data['id']}:{option_data['text']}")
        elif "id" in option_data and "text" in option_data:
             if "priority" not in option_data:
                 lines.append(f"{option_data['id']}:{option_data['text']}")

    return '\n'.join(lines)
# ------------------------------------------------------------------------------------

def csv_to_json(csv_filepath, json_filepath):
    """Конвертирует CSV в JSON, сохраняя структуру строк."""
    json_data = []
    row_num = 1
    print(f"Reading CSV: {csv_filepath} with encoding {CSV_ENCODING}")
    expected_columns = 0 # Определим по заголовку
    header = []
    try:
        with open(csv_filepath, 'r', encoding=CSV_ENCODING, newline='') as csvfile:
            reader = csv.reader(csvfile)
            try:
                header = next(reader)
                expected_columns = len(header)
                print(f"CSV Header: {header} ({expected_columns} columns)")
                row_num += 1
            except StopIteration:
                print("Error: CSV file is empty or has no header.")
                return

            for i, row in enumerate(reader):
                current_row_processing = row_num + i
                # Сохраняем все поля как список, дополняя/обрезая до expected_columns
                fields = list(row)
                if len(fields) < expected_columns:
                    fields.extend([''] * (expected_columns - len(fields)))
                elif len(fields) > expected_columns:
                    fields = fields[:expected_columns]

                row_obj = {
                    "_row_number": current_row_processing,
                    "fields": fields # Сохраняем нормализованный список полей
                }

                # Определяем тип строки
                is_comment = bool(fields) and fields[0].strip().startswith('#')
                is_empty_or_separator = not any(f.strip() for f in fields)

                if is_comment:
                    row_obj["_type"] = "comment"
                elif is_empty_or_separator:
                    row_obj["_type"] = "empty_separator"
                else:
                    # Это строка данных, добавляем парсинг
                    row_obj["_type"] = "data"
                    for idx, field_name in enumerate(header):
                        value = fields[idx]
                        if field_name == 'options':
                            # Сохраняем распарсенные options *дополнительно* к raw fields
                            row_obj[field_name] = parse_options_string(value)
                        else:
                            # Сохраняем значение поля для удобства доступа
                            row_obj[field_name] = value

                json_data.append(row_obj)

    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_filepath}")
        return
    except UnicodeDecodeError as e:
        print(f"\n!!! Error: Failed to decode CSV file using encoding '{CSV_ENCODING}'. !!!")
        print(f"!!! Please check the CSV_ENCODING setting. Error: {e} !!!\n")
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

def json_to_csv(json_filepath, output_csv_filepath): # Убран original_csv_filepath
    """Собирает новый CSV из JSON данных."""
    print(f"Reading processed JSON: {json_filepath} with encoding {JSON_ENCODING}")
    try:
        with open(json_filepath, 'r', encoding=JSON_ENCODING) as jsonfile:
            json_data_list = json.load(jsonfile)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {json_filepath}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON file: {e}")
        return
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    # Заголовок получаем из первого объекта данных или предполагаем стандартный
    header = None
    expected_columns = 7 # По умолчанию
    # Ищем первый объект data, чтобы получить из него имена полей заголовка
    for item in json_data_list:
        if item.get("_type") == "data":
            temp_header = [k for k in item.keys() if not k.startswith('_') and k != 'fields']
            # Проверяем, что это стандартные поля
            if all(f in temp_header for f in ['id', 'trigger', 'conditions', 'script', 'text', 'options', 'notes']):
                 header = ['id', 'trigger', 'conditions', 'script', 'text', 'options', 'notes'] # Используем стандартный порядок
                 expected_columns = len(header)
                 break
            else: # Если поля нестандартные, берем список полей
                 if "fields" in item and len(item["fields"]) > 0:
                      expected_columns = len(item["fields"])
                      header = [f"field_{i}" for i in range(expected_columns)] # Генерируем имена
                      print(f"Warning: Could not determine standard header, using generic field names based on field count ({expected_columns}).")
                      break

    if not header:
        print("Warning: No data found in JSON to determine header. Using default 7 columns.")
        header = ['id', 'trigger', 'conditions', 'script', 'text', 'options', 'notes']
        expected_columns = len(header)


    print(f"Writing output CSV: {output_csv_filepath} with encoding {OUTPUT_CSV_ENCODING}")

    current_row_num = 0
    try:
        with open(output_csv_filepath, 'w', encoding=OUTPUT_CSV_ENCODING, newline='') as outfile:
            # Используем QUOTE_ALL для надежности экранирования
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

            writer.writerow(header)
            current_row_num = 1

            for item in json_data_list:
                current_row_num = item.get("_row_number", current_row_num + 1)
                row_type = item.get("_type")

                if row_type in ["comment", "empty_separator", "potentially_empty", "malformed_row"]:
                    # Записываем сохраненные поля из "fields"
                    fields_to_write = item.get("fields", [''] * expected_columns)
                    # Обеспечиваем правильное количество полей
                    if len(fields_to_write) < expected_columns:
                        fields_to_write.extend([''] * (expected_columns - len(fields_to_write)))
                    elif len(fields_to_write) > expected_columns:
                        fields_to_write = fields_to_write[:expected_columns]
                    writer.writerow(fields_to_write)

                elif row_type == "data":
                    # Собираем строку из данных JSON объекта, используя порядок заголовка
                    output_row = []
                    for field_name in header:
                        if field_name == 'options':
                            # Собираем поле options из структурированного вида
                            options_list = item.get(field_name, []) # Получаем список [{id:'..', text:'..'}, ...]
                            output_row.append(build_options_string(options_list))
                        else:
                            # Получаем значение поля из объекта JSON
                            output_row.append(item.get(field_name, ''))
                    # Проверка количества колонок не нужна, т.к. собираем по заголовку
                    writer.writerow(output_row)

                # else: Пропускаем неизвестные типы

        print("JSON to CSV conversion successful.")

    except Exception as e:
        print(f"Error during JSON to CSV conversion (around row {current_row_num}): {e}")
        import traceback
        traceback.print_exc()

# --- Основной блок ---
if __name__ == "__main__":
    # ... (блок запуска остается тем же, но ORIGINAL_CSV_FILE больше не нужен для json2csv) ...
    if MODE == 'csv2json':
        print(f"Starting CSV to JSON conversion...")
        print(f"  Input CSV: {CSV_INPUT_FILE}")
        print(f"  Output JSON: {JSON_FILE}")
        csv_to_json(CSV_INPUT_FILE, JSON_FILE)
    elif MODE == 'json2csv':
        print(f"Starting JSON to CSV conversion...")
        print(f"  Input JSON: {JSON_FILE}")
        print(f"  Output CSV: {CSV_OUTPUT_FILE}")
        json_to_csv(JSON_FILE, CSV_OUTPUT_FILE) # Убран original_csv_filepath из вызова
    else:
        print(f"Error: Unknown MODE '{MODE}'. Please set MODE to 'csv2json' or 'json2csv'.")

    print("Script finished.")