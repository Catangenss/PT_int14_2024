import pytest
import tests
import sqlite3
from urllib.parse import quote, unquote
from unittest.mock import patch
from main import (
    initialize_database,
    insert_links,
    parse_links,
    WikipediaLinkParser_v0,
    WikipediaLinkParser_v1
)


# --- Подключение к базе данных ---
def test_initialize_database_success():
    conn = initialize_database(":memory:")
    assert isinstance(conn, sqlite3.Connection)
    conn.close()


# --- Вставка ссылок ---
def test_insert_links_special_characters():
    conn = initialize_database(":memory:")
    table_name = "test_table"
    conn.execute(f"CREATE TABLE {table_name} (url TEXT UNIQUE, parent_url TEXT)")
    links = [
        ("http://example.com/%20", None),
        ("http://пример.рф", None),
        ("http://example.com/special?query=%25&value=1", None),
    ]
    insert_links(conn, table_name, links)

    cursor = conn.cursor()
    cursor.execute(f"SELECT url FROM {table_name}")
    urls = [row[0] for row in cursor.fetchall()]
    assert "http://example.com/ " in urls  # %20 -> пробел
    assert "http://пример.рф" in urls  # Поддержка кириллицы
    assert "http://example.com/special?query=%&value=1" in urls  # Декодирование %25
    conn.close()


def test_insert_links_empty():
    conn = initialize_database(":memory:")
    table_name = "test_table"
    conn.execute(f"CREATE TABLE {table_name} (url TEXT UNIQUE, parent_url TEXT)")
    insert_links(conn, table_name, [])
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchone()[0] == 0  # Никаких записей не добавлено
    conn.close()


def test_insert_links_long_url():
    conn = initialize_database(":memory:")
    table_name = "test_table"
    conn.execute(f"CREATE TABLE {table_name} (url TEXT UNIQUE, parent_url TEXT)")
    long_url = "http://example.com/" + "a" * 2048
    insert_links(conn, table_name, [(long_url, None)])
    cursor = conn.cursor()
    cursor.execute(f"SELECT url FROM {table_name}")
    urls = [row[0] for row in cursor.fetchall()]
    assert long_url in urls
    conn.close()


# --- Проверка валидации URL ---
def test_parse_links_non_wikipedia_url():
    with pytest.raises(RuntimeError, match="ссылка должна быть на Википедию"):
        parse_links("http://example.com", verbose=0)


# --- Кодирование и декодирование URL ---
def test_url_encoding_decoding():
    original_url = "http://example.com/special?query=привет&value=100"
    encoded_url = quote(original_url, safe=":/?=&")
    decoded_url = unquote(encoded_url)
    assert decoded_url == original_url


from unittest.mock import MagicMock

@patch("main.urlopen")
def test_parse_links_excludes_invalid(mock_urlopen):
    mock_response = MagicMock()
    mock_response.status = 200  # Мокируем статус ответа
    mock_response.read.return_value = b"""
        <html>
            <body>
                <a href="/wiki/ValidLink"></a>
                <a href="/wiki/Invalid:Link"></a>
                <a href="/wiki/#Fragment"></a>
            </body>
        </html>
    """
    mock_urlopen.return_value = mock_response

@patch("main.urlopen")
def test_parse_links_verbose(mock_urlopen):
    mock_response = MagicMock()
    mock_response.status = 200  # Мокируем статус ответа
    mock_response.read.return_value = b"""
        <html>
            <body>
                <a href="/wiki/Link1"></a>
                <div role="navigation"><a href="/wiki/Link2"></a></div>
                <table><a href="/wiki/Link3"></a></table>
            </body>
        </html>
    """
    mock_urlopen.return_value = mock_response
    

# --- Обработка очереди ---
def test_queue_processing():
    queue = []
    queue.append("http://example.com/1")
    queue.append("http://example.com/2")
    queue.append("http://example.com/3")
    assert len(queue) == 3
    processed = queue.pop(0)
    assert processed == "http://example.com/1"
    assert len(queue) == 2


# --- Работа парсеров ---
def test_wikipedia_parser_v0():
    parser = WikipediaLinkParser_v0("https://ru.wikipedia.org/wiki/VIII_век")
    parser.feed("""
    <div class="mw-content-ltr mw-parser-output" lang="en" dir="ltr">
    <table class="infobox ib-settlement vcard">
        <tbody>
        <tr><th colspan="2" class="infobox-above">
            <div class="fn org">Sapogovo</div>
            <div class="nickname ib-settlement-native">Сапогово</div>
        </th></tr>
        <tr><td colspan="2" class="infobox-subheader">
            <div class="category">Village</div>
        </td></tr>
        <tr>
            <td colspan="2" class="infobox-full-data">
            Coordinates: <span class="geo-inline">
                <span class="plainlinks nourlexpansion">
                <a class="external text" href="https://geohack.toolforge.org/geohack.php?pagename=Sapogovo&amp;params=60_32_N_38_09_E_type:city_region:RU-VLG">
                    <span class="geo-default"><span class="geo-dms" title="Maps, aerial photos, and other data for this location"></span>
                </a>
                </span>
            </span>
            </span>
        </td>
        </tr>
        <tr><th scope="row" class="infobox-label">Country</th><td class="infobox-data"><a href="/wiki/Russia" title="Russia">Russia</a></td></tr>
        <tr><th scope="row" class="infobox-label"><a href="/wiki/List_of_regions_of_Russia" class="mw-redirect" title="List of regions of Russia">Region</a></th><td class="infobox-data"><a href="/wiki/Vologda_Oblast" title="Vologda Oblast">Vologda Oblast</a></td></tr>
        <tr><th scope="row" class="infobox-label"><a href="/wiki/Districts_of_Russia" title="Districts of Russia">District</a></th><td class="infobox-data"><a href="/wiki/Vashkinsky_District" title="Vashkinsky District">Vashkinsky District</a></td></tr>
        <tr><th scope="row" class="infobox-label"><a href="/wiki/Time_zone" title="Time zone">Time zone</a></th><td class="infobox-data"><a href="/wiki/UTC%2B3:00" class="mw-redirect" title="UTC+3:00">UTC+3:00</a></td></tr>
        </tbody>
    </table>

    <p><b>Sapogovo</b> (<a href="/wiki/Russian_language" title="Russian language">Russian</a>: <span lang="ru">Сапогово</span>) is a <a href="/wiki/Types_of_inhabited_localities_in_Russia" class="mw-redirect" title="Types of inhabited localities in Russia">rural locality</a> (a <a href="/wiki/Village#Russia" title="Village">village</a>) in Andreyevskoye Rural Settlement, <a href="/wiki/Vashkinsky_District" title="Vashkinsky District">Vashkinsky District</a>, <a href="/wiki/Vologda_Oblast" title="Vologda Oblast">Vologda Oblast</a>, Russia. The population was 1 as of 2002.</p>

    <div role="navigation" class="navbox" aria-labelledby="Rural_localities_in_Vashkinsky_District" style="padding:3px">
        <table class="nowraplinks mw-collapsible autocollapse navbox-inner mw-made-collapsible" style="border-spacing:0;background:transparent;color:inherit">
        <tbody>
            <tr><th scope="col" class="navbox-title" colspan="4">
            <button type="button" class="mw-collapsible-toggle mw-collapsible-toggle-default" aria-expanded="true" tabindex="0"><span class="mw-collapsible-text">hide</span></button>
            <div id="Rural_localities_in_Vashkinsky_District" style="font-size:114%;margin:0 4em"><a href="/wiki/Classification_of_inhabited_localities_in_Russia" title="Classification of inhabited localities in Russia">Rural localities</a> in <a href="/wiki/Vashkinsky_District" title="Vashkinsky District">Vashkinsky District</a></div>
            </th></tr>
            <tr><td colspan="2" class="navbox-list navbox-odd hlist" style="width:100%;padding:0">
            <ul>
                <li><a href="/wiki/Aksentyevo,_Vashkinsky_District,_Vologda_Oblast" title="Aksentyevo, Vashkinsky District, Vologda Oblast">Aksentyevo</a></li>
                <li><a href="/wiki/Aleshino,_Vashkinsky_District,_Vologda_Oblast" title="Aleshino, Vashkinsky District, Vologda Oblast">Aleshino</a></li>
                <li><a href="/wiki/Alferovskaya" title="Alferovskaya">Alferovskaya</a></li>
                <li><a href="/wiki/Andreyevskaya,_Vashkinsky_District,_Vologda_Oblast" title="Andreyevskaya, Vashkinsky District, Vologda Oblast">Andreyevskaya</a></li>
                <li><a href="/wiki/Anikovo,_Vashkinsky_District,_Vologda_Oblast" title="Anikovo, Vashkinsky District, Vologda Oblast">Anikovo</a></li>
            </ul>
            </td></tr>
        </tbody>
        </table>
    </div>
    </div>
    <a href="/wiki/Main_Page" title="Visit the main page [alt-shift-z]" accesskey="z"><span>Main page</span></a>
    <a href="/wiki/Main_Page" title="Visit the main page [alt-shift-z]" accesskey="z"><span>Main page</span></a>
    """)
    links = parser.get_links()
    assert len(links) == 4
    assert "https://ru.wikipedia.org/wiki/Russian_language" in links

# --- Обработка некорректного HTML ---
def test_handle_invalid_html():
    parser = WikipediaLinkParser_v1("http://example.com")
    try:
        parser.feed("<div><a href='/wiki/Link1'></div>")
    except Exception:
        tests.fail("Парсер не должен падать на некорректной разметке")
