from tokenize_uk import tokenize_words
import re
import pymorphy3
import sqlite3


# функція для токенізації і фільтрування тексту
def tokenize_text(file_path):
    with open(file_path, 'r', encoding='utf-8') as data:
        text = data.read().lower()

    tokens = tokenize_words(text)
    filtered_tokens = [token for token in tokens if re.match(r'\w+', token)]
    return filtered_tokens


# створення словника для зберігання частот словоформ
def process_tokens(tokens):
    sample_dict = {}
    morph = pymorphy3.MorphAnalyzer(lang='uk')

    # створення підвибірок і обчислення частот
    for sample_number in range(20):
        start_index = sample_number * 1000
        end_index = start_index + 1000
        sample_list = tokens[start_index:end_index]

        for token in sample_list:
            morph_info = morph.parse(token)[0]
            lemma = morph_info.normal_form
            pos = morph_info.tag.POS

            if token in sample_dict:
                if len(sample_dict[token]['частоти']) <= sample_number:
                    sample_dict[token]['частоти'] += [0] * (sample_number - len(sample_dict[token]['частоти'])) + [1]
                else:
                    sample_dict[token]['частоти'][sample_number] += 1
            else:
                sample_dict[token] = {
                    'словоформа': token,
                    'лема': lemma,
                    'частина мови': pos,
                    'частоти': [0] * sample_number + [1]
                }

    return sample_dict


# створення бази даних
def create_database(file_path, sample_dict):
    global columns_str, values_ordered  # глобальні змінні для подальшого використання
    db_name = f"{file_path.split('.')[0]}.db"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # генерація стовпців для частот
    frequency_columns = [f'підв_{i} INTEGER' for i in range(1, 21)]
    columns_str = ', '.join(frequency_columns)

    # створення проміжної таблиці
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS проміжна_таблиця (
        словоформа TEXT,
        лема TEXT,
        частина_мови TEXT,
        абсолютна_частота INTEGER,
        {columns_str}
    );
    ''')

    # формування списку для вставки даних
    values = []
    for entry in sample_dict.values():
        total_frequency = sum(entry['частоти'])
        entry_values = [entry['словоформа'], entry['лема'], entry['частина мови'], total_frequency] + entry['частоти']

        # якщо кількість елементів менше 24, додаємо стовпці з 0
        while len(entry_values) < 24:
            entry_values.append(0)

        values.append(entry_values)

    # сортування даних за абсолютною частотою
    values_ordered = sorted(values, key=lambda x: x[3], reverse=True)

    # вставка даних у таблицю
    for entry in values_ordered:
        cursor.execute('''
        INSERT INTO проміжна_таблиця
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        ''', entry)

    conn.commit()
    conn.close()


# список файлів для обробки
file_paths = [
    "Istoria_zaporizkykh_kozakiv_Tom1.txt",
    "Biblia_Staryi_zapovit.txt"
]

# обробка кожного файлу і створення бази даних
for file_path in file_paths:
    tokens = tokenize_text(file_path)[:20000]  # токенізація
    sample_dict = process_tokens(tokens)  # обчислення частот
    create_database(file_path, sample_dict)  # створення бази даних


# функція для створення таблиці
def create_table(cursor, table_name, unique_col):
    cursor.execute(f'''
  CREATE TABLE IF NOT EXISTS {table_name} (
    {unique_col},
    абсолютна_частота INTEGER,
    {columns_str}
  );
  ''')


# функція для вставки або оновлення даних у таблиці
def insert_or_replace_data(cursor, table_name, values):
    cursor.execute(f'''
  INSERT OR REPLACE INTO {table_name}
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
  ''', values)


def select_entry(cursor, name):
    # Генерація частотних стовпців
    frequency_columns = [f'SUM(підв_{i}) AS підв_{i}' for i in range(1, 21)]

    # Об'єднання частотних стовпців у рядок
    frequency_columns_str = ', '.join(frequency_columns)

    # Повний запит
    cursor.execute(f'''
    SELECT {name},
      SUM(абсолютна_частота) AS абсолютна_частота,
      {frequency_columns_str}
    FROM проміжна_таблиця
    GROUP BY {name}
    ORDER BY абсолютна_частота DESC;
    ''')
    return cursor.fetchall()


# функція для обробки даних запиту та вставки результатів у таблицю
def process_and_insert_data(cursor, table_name, data):
    for row in data:
        main_value = row[0]
        absolute_frequency = row[1]
        subsets = row[2:]
        insert_or_replace_data(cursor, table_name, (main_value, absolute_frequency) + tuple(subsets))


db_paths = [
    "Istoria_zaporizkykh_kozakiv_Tom1.db",
    "Biblia_Staryi_zapovit.db"
]

for db_path in db_paths:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # унікальні колонки кожної таблиці
    wordform_column = 'словоформа TEXT PRIMARY KEY'
    lemma_column = 'лема TEXT PRIMARY KEY'
    pos_column = 'частина_мови TEXT PRIMARY KEY'

    # створення таблиць
    create_table(cursor, 'чс_словоформ', wordform_column)
    create_table(cursor, 'чс_лем', lemma_column)
    create_table(cursor, 'чс_частин_мови', pos_column)

    wordforms = select_entry(cursor, 'словоформа')
    process_and_insert_data(cursor, 'чс_словоформ', wordforms)

    lemmas = select_entry(cursor, 'лема')
    process_and_insert_data(cursor, 'чс_лем', lemmas)

    pos = select_entry(cursor, 'частина_мови')
    process_and_insert_data(cursor, 'чс_частин_мови', pos)

    conn.commit()
    conn.close()
