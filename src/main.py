import pyodbc
from woocommerce import API
from requests.exceptions import HTTPError
import logging
from dotenv import load_dotenv
import os

# Konfiguracja logowania
from logging_formatter import CustomFormatter
log = logging.getLogger("My_app")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
log.addHandler(ch)

# env
load_dotenv()

# Zmienne globalne
cursor = None
wcapi = None

__conn = None
__cursor = None
def get_database_connection():
    """
    Nawiązuje połączenie z bazą danych MSSQL (singleton) używając pyodbc.
    Returns:
        pyodbc.Cursor: Kursor do bazy danych MSSQL.
    """
    global __conn, __cursor
    if __cursor is not None:
        return __cursor
    try:
        log.debug(f"Łączenie z bazą danych MSSQL na hoście {os.getenv('database_host')}")
        
        host = os.getenv('database_host')  # e.g., 192.168.179.150\OPTIMA
        database = os.getenv('database_name')
        user = os.getenv('database_user')
        password = os.getenv('database_password')
        domain = os.getenv('database_domain', '')

        conn_str_parts = [
            "DRIVER=/usr/lib/libtdsodbc.so",
            f"SERVER={host}",
            f"DATABASE={database}",
            "TrustServerCertificate=yes",
        ]

        if domain: conn_str_parts.append(f"UID={domain}\\{user}")
        else: conn_str_parts.append(f"UID={user}")
        
        conn_str_parts.append(f"PWD={password}")

        connection_string = ";".join(conn_str_parts)

        __conn = pyodbc.connect(connection_string)
        __cursor = __conn.cursor()
        log.info("Połączono z bazą danych MSSQL (pyodbc).")
        return __cursor
    except Exception as e:
        log.error(f"Błąd połączenia z bazą danych: {e}")
        raise

__wcapi = None
def get_woocommerce_api():
    """
    Nawiązuje połączenie z WooCommerce API (singleton).
    Returns:
        woocommerce.API: Obiekt API WooCommerce.
    """
    global __wcapi
    if __wcapi is not None:
        return __wcapi
    try:
        __wcapi = API(
            url=os.getenv("woocommerce_store_url"),
            consumer_key=os.getenv("woocommerce_consumer_key"),
            consumer_secret=os.getenv("woocommerce_consumer_secret"),
            wp_api=True,
            version="wc/v3",
        )
        log.info("Połączono z WooCommerce API.")
        return __wcapi
    except Exception as e:
        log.error(f"Błąd połączenia z WooCommerce API: {e}")
        raise

def main():
    # Połączenie z bazą danych i WooCommerce API
    global cursor, wcapi
    try:
        cursor = get_database_connection()
        wcapi = get_woocommerce_api()
    except Exception as e:
        log.error(f"Błąd podczas łączenia się z zasobami: {e}")
        raise

    # Synchronizacja produktów
    sync_products()

def sync_products():
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    """
    try:
        # Przykładowe zapytanie do bazy danych
        cursor.execute("SELECT Twr_Nazwa, Twr_Opis, Twr_CenaZCzteremaMiejscami FROM CDN.Towary")
        products = cursor.fetchall()

        for product in products:
            data = {
                "name": product[0],
                "description": product[1],
                "regular_price": str(product[2])
            }
            try:
                log.debug(f"Produkt: {data}")

                # response = wcapi.post("products", data).json()
                # if not response.get("id"):
                #     log.error(f"Błąd podczas synchronizacji produktu {product[0]}: {response}")
                #     continue

                log.info(f"Produkt {product[0]} zsynchronizowany z WooCommerce.")
            except HTTPError as http_err:
                log.error(f"Błąd HTTP podczas synchronizacji produktu {product[0]}: {http_err}")
        log.info("Zakończono synchronizacje produktów.")
    except Exception as e:
        log.error(f"Błąd podczas synchronizacji produktów: {e}")
        raise
    
    
if __name__ == "__main__":
    main()