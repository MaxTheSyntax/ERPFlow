import logger as log
from dotenv import load_dotenv
import os
import pyodbc
from woocommerce import API

__all__ = ["cursor", "wcapi", "conn"]

__conn = None
__cursor = None
def __get_database_connection():
    """
    Nawiązuje połączenie z bazą danych MSSQL (singleton) używając pyodbc.
    Returns:
        pyodbc.Cursor: Kursor do bazy danych MSSQL.
    """
    global __conn, __cursor
    if __cursor is not None and __conn is not None:
        return __cursor, __conn
    try:
        log.debug(f"Łączenie z bazą danych MSSQL na hoście {os.getenv('database_host')}")
        
        host = os.getenv('database_host') 
        database = os.getenv('database_name')
        user = os.getenv('database_user')
        password = os.getenv('database_password')
        domain = os.getenv('database_domain')
        driver = os.getenv('database_driver', '{ODBC Driver 17 for SQL Server}')

        conn_str_parts = [
            f"DRIVER={driver}",
            f"SERVER={host}",
            f"DATABASE={database}",
            "TrustServerCertificate=yes",
        ]

        if user and password:
            # SQL auth
            if domain: conn_str_parts.append(f"UID={domain}\\{user}")
            else: conn_str_parts.append(f"UID={user}")
            conn_str_parts.append(f"PWD={password}")
        else:
            # Windows auth
            conn_str_parts.append("Trusted_Connection=yes")

        connection_string = ";".join(conn_str_parts)

        __conn = pyodbc.connect(connection_string)
        __conn.autocommit = True
        __cursor = __conn.cursor()
        log.info("Połączono z bazą danych MSSQL (pyodbc).")
        return __cursor, __conn
    except Exception as e:
        log.error(f"Błąd połączenia z bazą danych: {e}")
        raise

__wcapi = None
def __get_woocommerce_api():
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
            timeout=30
        )
        log.info("Połączono z WooCommerce API.")
        return __wcapi
    except Exception as e:
        log.error(f"Błąd połączenia z WooCommerce API: {e}")
        raise

def initialize():
    global cursor, conn, wcapi
    load_dotenv()
    cursor, conn = __get_database_connection()
    wcapi = __get_woocommerce_api()

cursor = None
conn = None
wcapi = None