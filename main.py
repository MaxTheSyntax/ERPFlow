import pyodbc
from woocommerce import API
from requests.exceptions import HTTPError
import logging

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)

cursor = None
wcapi = None

__cursor = None
def get_database_connection():
    """
    Nawiązuje połączenie z bazą danych MSSQL (singleton).
    Returns:
        pyodbc.Cursor: Kursor do bazy danych MSSQL.
    """
    global __cursor
    if __cursor is not None:
        return __cursor
    try:
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=your_server;'
            'DATABASE=your_database;'
            'UID=your_username;'
            'PWD=your_password'
        )
        __cursor = cnxn.cursor()
        logging.info("Połączono z bazą danych MSSQL.")
        return __cursor
    except Exception as e:
        logging.error(f"Błąd połączenia z bazą danych: {e}")
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
            url="https://your-store.com",
            consumer_key="your_consumer_key",
            consumer_secret="your_consumer_secret",
            version="wc/v3"
        )
        logging.info("Połączono z WooCommerce API.")
        return __wcapi
    except Exception as e:
        logging.error(f"Błąd połączenia z WooCommerce API: {e}")
        raise

def main():
    # Połączenie z bazą danych i WooCommerce API
    global cursor, wcapi
    try:
        cursor = get_database_connection()
        wcapi = get_woocommerce_api()
        logging.info("Uruchomiono główną funkcję synchronizacji.")
    except Exception as e:
        logging.error(f"Błąd podczas łączenia się z zasobami: {e}")
        raise

def sync_products():
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    """
    try:
        # Przykładowe zapytanie do bazy danych
        cursor.execute("SELECT Twr_Nazwa, Twr_Opis, Twr_CenaZCzteremaMiejscami FROM Towary")
        products = cursor.fetchall()

        for product in products:
            data = {
                "name": product.Twr_Nazwa,
                "description": product.Twr_Opis,
                "regular_price": str(product.Twr_CenaZCzteremaMiejscami)
            }
            try:
                response = wcapi.post("products", data).json()
                response.raise_for_status()
                logging.info(f"Produkt {product.Twr_Nazwa} zsynchronizowany z WooCommerce.")
            except HTTPError as http_err:
                logging.error(f"Błąd HTTP podczas synchronizacji produktu {product.Twr_Nazwa}: {http_err}")
        logging.info("Zakończono synchronizacje produktów.")
    except HTTPError as http_err:
        logging.error(f"Błąd HTTP podczas synchronizacji produktów: {http_err}")
        raise
    except Exception as e:
        logging.error(f"Błąd podczas synchronizacji produktów: {e}")
        raise
    
    
if __name__ == "__main__":
    main()