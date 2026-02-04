import pyodbc
from woocommerce import API
from requests.exceptions import HTTPError
import logging
from dotenv import load_dotenv
import os
import argparse

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
args = None
conn = None

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
        log.debug(f"Używamy: {conn_str_parts}")

        connection_string = ";".join(conn_str_parts)

        __conn = pyodbc.connect(connection_string)
        __cursor = __conn.cursor()
        log.info("Połączono z bazą danych MSSQL (pyodbc).")
        return __cursor, __conn
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
    parser = argparse.ArgumentParser(
        description="Synchronizacja produktów między bazą danych MSSQL a WooCommerce."
    )
    parser.add_argument(
        "--obejmuj-darmowe-towary",
        dest="obejmuj_darmowe_towary",
        action="store_true",
        default=False,
        help="Synchronizuj również darmowe towary (cena = 0). Domyślnie wyłączone."
    )
    parser.add_argument(
        "--setup",
        dest="setup",
        action="store_true",
        default=False,
        help="Skonfiguruj bazę danych do śledzenia zmian."
    )
    
    global args
    args = parser.parse_args()
    
    # Połączenie z bazą danych i WooCommerce API
    global cursor, conn, wcapi
    try:
        cursor, conn = get_database_connection()
        wcapi = get_woocommerce_api()
    except Exception as e:
        log.error(f"Błąd podczas łączenia się z zasobami: {e}")
        raise

    # Konfiguracja bazy danych do śledzenia zmian jeśli flaga jest ustawiona
    if args.setup:
        setup()
        return

    # Synchronizacja produktów
    sync_products()

def is_already_enabled_error(error):
    """
    Sprawdza czy błąd oznacza że change tracking jest już włączone.
    """
    msg = str(error).lower()
    return "change tracking" in msg and ("already enabled" in msg or "already been enabled" in msg)

def setup():
    """
    Konfigugurje baze danych do synchronizacji.
    Obsługuje przypadki częściowego włączenia śledzenia zmian.
    """
    database_name = os.getenv("database_name")
    tracked_tables = ["Towary", "TwrCeny"]
    
    try:
        # Włączenie change tracking nie działa z fiskalizacją, więc włączamy autocommit.
        conn.autocommit = True
        try:
            cursor.execute(f'''ALTER DATABASE [{database_name}] SET CHANGE_TRACKING = ON (AUTO_CLEANUP = ON, CHANGE_RETENTION = 2 DAYS);''')
            log.debug(f"Śledzenie zmian włączone dla bazy danych '{database_name}'.")
        except pyodbc.Error as db_error:
            if is_already_enabled_error(db_error):
                log.warning(f"Śledzenie zmian jest już włączone dla bazy danych '{database_name}'.")
            else:
                raise
        
        # Włącz śledzenie zmian dla każdej tabeli osobno używając w pełni kwalifikowanych nazw
        for table in tracked_tables:
            try:
                cursor.execute(f'''ALTER TABLE [{database_name}].[CDN].[{table}] ENABLE CHANGE_TRACKING;''')
                log.debug(f"Włączono śledzenie zmian dla tabeli '{table}'.")
            except pyodbc.Error as table_error:
                if is_already_enabled_error(table_error):
                    log.warning(f"Śledzenie zmian jest już włączone dla tabeli '{table}'.")
                else:
                    raise

        conn.autocommit = False
        log.info("Konfiguracja śledzenia zmian zakończona pomyślnie.")
    except pyodbc.Error as e:
        log.error(f"Błąd podczas konfiguracji bazy danych do śledzenia zmian: {e}")
        raise

def sync_products():
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    """
    try:
        query = '''
            SELECT DISTINCT Twr_Nazwa, Twr_Opis, TwC_Wartosc, TwC_Zaokraglenie FROM CDN.Towary t
            INNER JOIN CDN.TwrCeny tc ON t.Twr_TwrId = tc.TwC_TwrID
            WHERE tc.TwC_Typ = 2
        '''
        
        cursor.execute(query)
        products = cursor.fetchall()

        products_to_create = []        
        for product in products:
            regular_price = str(round(round(product.TwC_Wartosc / product.TwC_Zaokraglenie) * product.TwC_Zaokraglenie, 2))
            
            # Pomijamy darmowe towary jeśli flaga nie jest ustawiona
            if float(regular_price) == 0 and not args.obejmuj_darmowe_towary:
                log.warning(f"Pominięto darmowy produkt '{product.Twr_Nazwa}'. Użyj --obejmuj-darmowe-towary, aby zsynchronizować również darmowe towary.")
                continue
            
            product_data = {
                "name": product.Twr_Nazwa,
                "description": product.Twr_Opis,
                "regular_price": regular_price
            }
            products_to_create.append(product_data)
            log.debug(f"Przygotowano produkt do synchronizacji: {product_data}")

        # WooCommerce ma limit 100 produktów na żądanie
        batch_size = 100
        total_synced = 0
        
        for i in range(0, len(products_to_create), batch_size):
            batch = products_to_create[i:i + batch_size]
            
            data = {
                "create": batch
            }
            
            try:
                response = wcapi.post("products/batch", data).json()
                
                # Sprawdzenie wyników
                created = response.get("create", [])
                errors = [item for item in created if item.get("error")]
                successful = [item for item in created if not item.get("error") and item.get("id")]
                
                total_synced += len(successful)
                
                for item in successful:
                    log.info(f"Produkt '{item.get('name', 'N/A')}' zsynchronizowany z WooCommerce (ID: {item.get('id')}).")
                
                for error in errors:
                    log.error(f"Błąd podczas synchronizacji produktu: {error}")
                    
            except HTTPError as http_err:
                log.error(f"Błąd HTTP podczas synchronizacji partii produktów: {http_err}")
            except Exception as e:
                log.error(f"Błąd podczas synchronizacji partii produktów: {e}")
        
        log.info(f"Zakończono synchronizacje produktów. Zsynchronizowano {total_synced}/{len(products_to_create)} produktów.")
    except Exception as e:
        log.error(f"Błąd podczas synchronizacji produktów: {e}")
    
    
if __name__ == "__main__":
    main()