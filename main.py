import sqlite3
from typing import List, Tuple, Optional
from urllib.request import urlopen
from urllib.parse import urljoin, quote, unquote
from html.parser import HTMLParser
import argparse
from collections import deque
import re

# Инициализация SQLite
def initialize_database(db_path: str) -> sqlite3.Connection:
    conn = connect_to_db(db_path)
    return conn

# Подключаеимся к SQLite
def connect_to_db(db_path: str) -> sqlite3.Connection:
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        raise RuntimeError(f"Ошибка подключения к базе данных: {e}")

# Создаем таблицу с уникальным именем
def create_table(conn: sqlite3.Connection) -> str:
    cursor = conn.cursor()
    
    # Проверяем существование таблицы
    def table_exists(table_name: str) -> bool:
        cursor.execute('''
            SELECT name FROM sqlite_master WHERE type='table' AND name=?
        ''', (table_name,))
        return cursor.fetchone() is not None
    
    # Определяем имя таблицы
    base_table_name = 'links'
    table_name = base_table_name
    counter = 0
    
    # Вычисляем имя следующей таблицы
    while table_exists(table_name):
        counter += 1
        table_name = f"{base_table_name}{counter}"
    
    # Создаем таблицу
    cursor.execute(f'''
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            parent_url TEXT
        )
    ''')
    conn.commit()
    print(f"Таблица создана с именем: {table_name}")
    
    # Возвращаем имя таблицы
    return table_name


# Вставляем ссылки из списка в указанную таблицу
def insert_links(conn: sqlite3.Connection, table_name: str, links: List[Tuple[str, Optional[str]]]) -> None:
    decoded_links = [(unquote(url), unquote(parent_url) if parent_url else None) for url, parent_url in links]
    cursor = conn.cursor()
    try:
        cursor.executemany(f'''
            INSERT OR IGNORE INTO {table_name} (url, parent_url) VALUES (?, ?) 
        ''', decoded_links) # Отсеиваем уже записанные ссылки
        conn.commit()
        # print(f"Данные успешно вставлены в таблицу {table_name}")
    except sqlite3.Error as e:
        raise RuntimeError(f"Ошибка записи в таблицу {table_name}: {e}")


class WikipediaLinkParser_v0(HTMLParser): # Парсер с фильтрацией блоков навигации и таблиц
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links = []
        self.in_navigation = False  # Флаг для нахождения в блоке role="navigation"
        self.div_count = 0  # Счетчик вложенных <div>
        self.in_infobox = False  # Флаг для нахождения в class infobox
        self.table_count = 0  # Счетчик вложенных <table>
        

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attrs_dict = dict(attrs)

        # Вход в блок с role="navigation"
        if attrs_dict.get("role") == "navigation":
            self.in_navigation = True
            self.div_count += 1  # Считаем первый открывающий div

        # Увеличиваем счетчик <div>, если входим в новый блок
        elif self.in_navigation and tag == "div":
            self.div_count += 1

        # Блокируем сбор ссылок из инфобоксов
        if tag == "table":
            self.in_infobox = True
            self.table_count +=1 

        # Добавляем ссылку в список в соответствии с параметров в --verbose
        if tag == "a":
            if not self.in_navigation and not self.in_infobox:
                href = attrs_dict.get("href")
                if href and href.startswith("/wiki/") and not any(x in href for x in [":", "#"]):
                    full_url = urljoin(self.base_url, href)
                    self.links.append(full_url)

    def handle_endtag(self, tag: str):
        # Уменьшаем счетчик при закрытии div внутри role="navigation"
        if self.in_navigation and tag == "div":
            self.div_count -= 1

        # Если счетчик div_count==0, выходим из блока role="navigation"
        if self.in_navigation and self.div_count == 0:
            self.in_navigation = False

        # Уменьшаем счетчик при закрытии table внутри class infobox
        if self.in_infobox and tag == "table":
            self.table_count -= 1
        
        # Если счетчик table_count==0, выходим из блока class infobox
        if self.in_infobox and self.table_count == 0:
            self.in_infobox = False

    def get_links(self) -> List[str]:
        self.links = self.links[:-2] # Исключаем заглавную страницу
        return self.links


class WikipediaLinkParser_v1(HTMLParser): # Парсер с фильтрацией блоков навигации
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links = []
        self.in_navigation = False  # Флаг для нахождения в блоке role="navigation"
        self.div_count = 0  # Счетчик вложенных <div>
        
    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attrs_dict = dict(attrs)

        # Вход в блок с role="navigation"
        if attrs_dict.get("role") == "navigation":
            self.in_navigation = True
            self.div_count += 1  # Считаем первый открывающий div

        # Увеличиваем счетчик <div>, если входим в новый блок
        elif self.in_navigation and tag == "div":
            self.div_count += 1

        # Добавляем ссылку в список в соответствии с параметров в --verbose
        if tag == "a":
            if not self.in_navigation:
                href = attrs_dict.get("href")
                if href and href.startswith("/wiki/") and not any(x in href for x in [":", "#"]):
                    full_url = urljoin(self.base_url, href)
                    self.links.append(full_url)

    def handle_endtag(self, tag: str):
        # Уменьшаем счетчик при закрытии div внутри role="navigation"
        if self.in_navigation and tag == "div":
            self.div_count -= 1

        # Если счетчик div_count==0, выходим из блока role="navigation"
        if self.in_navigation and self.div_count == 0:
            self.in_navigation = False

    def get_links(self) -> List[str]:
        self.links = self.links[:-2] # Исключаем заглавную страницу
        return self.links

class WikipediaLinkParser_v2(HTMLParser): # Парсер без фильтрации
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.links = []
        
    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attrs_dict = dict(attrs)

        # Добавляем ссылку в список в соответствии с параметров в --verbose
        if tag == "a":
            href = attrs_dict.get("href")
            if href and href.startswith("/wiki/") and not any(x in href for x in [":", "#"]):
                full_url = urljoin(self.base_url, href)
                self.links.append(full_url)


    def get_links(self) -> List[str]:
        self.links = self.links[:-2] # Исключаем заглавную страницу
        return self.links



# Получаем страницы и отправляем в парсер
def parse_links(url: str, verbose: int) -> List[str]:
    # Регулярное выражение для проверки ссылок на Википедию
    if not re.match(r'^https://[a-z]{2,3}\.wikipedia\.org/', url):
        raise RuntimeError("ссылка должна быть на Википедию")
    try:
        encoded_url = quote(url, safe=":/")
        response = urlopen(encoded_url)
        if response.status != 200:
            raise RuntimeError(f"Ошибка загрузки страницы: {response.status}")
        content = response.read().decode("utf-8")
        if verbose == 0:
            parser = WikipediaLinkParser_v0(url)
        elif verbose == 1:
            parser = WikipediaLinkParser_v1(url)
        else:
            parser = WikipediaLinkParser_v2(url)
        parser.feed(content)
        return parser.get_links()
    except Exception as e:
        raise RuntimeError(f"Ошибка при обработке URL {url}: {e}")


# Обработка рекурсии
def process_links_recursively(start_url: str, verbose: int, depth: int, conn: sqlite3.Connection):
    queue = deque([(start_url, 0)])  # Очередь (URL, текущая глубина)
    visited = set()  # Хранение обработанных ссылок

    table_name = create_table(conn)

    try:
        insert_links(conn, table_name, [(start_url, None)])
        print(f"Стартовый URL {start_url} добавлен в таблицу {table_name}.")
    except RuntimeError as e:
        print(f"Ошибка при добавлении стартового URL: {e}")
        return

    while queue:
        current_url, current_depth = queue.popleft()
        current_url = unquote(current_url)
        if current_depth >= depth:
            continue

        print(f"Обработка {current_url} на уровне {current_depth}...")
        visited.add(current_url)

        try:
            links = parse_links(current_url, verbose)
            links_to_insert = [(link, current_url) for link in links]
            insert_links(conn, table_name, links_to_insert)

            for link in links:
                if link not in visited:
                    queue.append((link, current_depth + 1))
                    visited.add(link)  # Переносим сюда для немедленной отметки
        except RuntimeError as e:
            print(f"Ошибка при обработке {current_url}: {e}")


# Парсим аргументы командной строки
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Скрипт для парсинга ссылок из статей Википедии")
    parser.add_argument("url", help="Ссылка на статью Википедии")
    parser.add_argument("-v", "--verbose", type=int, choices=[0, 1, 2], default=0,
                        help="Уровень фильтрации ссылок (0: без инфобоксов и навигации, 1: без навигации, 2: все ссылки)")
    parser.add_argument("-d", "--depth", type=int, default=6,
                        help="Глубина рекурсивного поиска (по умолчанию 6)")
    return parser.parse_args()


# Главная функция
def main():
    args = parse_arguments()
    db_path = "pt_int14.db"

    # Проверяем, что URL относится к Википедии
    if "wikipedia.org" not in args.url:
        print("Ошибка: ссылка должна быть на Википедию.")
        return

    conn = None
    try:
        conn = initialize_database(db_path)

        if args.depth > 1:
            process_links_recursively(args.url, args.verbose, args.depth, conn)
        else:
            # Для глубины 1, обработка без рекурсии
            process_links_recursively(args.url, args.verbose, 1, conn)

    except RuntimeError as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()