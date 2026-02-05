import time
import pyodbc
from woocommerce import API
from requests.exceptions import HTTPError
import logging
from dotenv import load_dotenv
import os
import argparse
import json

# Ścieżka do pliku JSON przechowującego wersje śledzenia zmian
CHANGE_TRACKING_FILE = os.path.join(os.path.dirname(__file__), "change_tracking_versions.json")

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

# Słownik przechowujący wersje śledzenia zmian (ładowany/zapisywany do JSON)
change_tracking_versions = {}

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

def batch_sync_products(creations: list[dict] = None, updates: list[dict] = None, deletions: list[int] = None) -> tuple[bool, list[dict], list[dict], list[dict]]:
    """
    Wysyła batchowe żądania do WooCommerce API dla tworzenia, aktualizacji i usuwania produktów.
    Wszystkie trzy operacje mogą być wykonane w jednym żądaniu batch.
    
    Args:
        creations: Lista słowników z danymi produktów do utworzenia.
        updates: Lista słowników z danymi produktów do zaktualizowania (musi zawierać 'id').
        deletions: Lista ID produktów WooCommerce do usunięcia.
    
    Returns:
        Tuple (success, created_items, updated_items, deleted_items) gdzie:
        - success: True jeśli wszystkie operacje zakończyły się sukcesem, False w przeciwnym razie
        - created_items: Lista utworzonych produktów z odpowiedzi API
        - updated_items: Lista zaktualizowanych produktów z odpowiedzi API
        - deleted_items: Lista usuniętych produktów z odpowiedzi API
    """
    creations = creations or []
    updates = updates or []
    deletions = deletions or []
    
    if not creations and not updates and not deletions:
        log.debug("Brak danych do synchronizacji z WooCommerce.")
        return True, [], [], []
    
    batch_size = 100  # WooCommerce ma limit 100 produktów na żądanie
    status = True
    all_created = []
    all_updated = []
    all_deleted = []
    
    total_operations = len(creations) + len(updates) + len(deletions)
    log.debug(f"Rozpoczynanie operacji w WooCommerce: {len(creations)} utworzeń, {len(updates)} aktualizacji, {len(deletions)} usunięć.")
    
    # Indeksy do śledzenia progressu w każdej liście
    create_idx = 0
    update_idx = 0
    delete_idx = 0
    
    while create_idx < len(creations) or update_idx < len(updates) or delete_idx < len(deletions):
        # Budujemy batch
        data = {}
        batch_created_count = 0
        batch_updated_count = 0
        batch_deleted_count = 0
        
        # Dodajemy tworzenia
        if create_idx < len(creations):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            batch = creations[create_idx:create_idx + remaining]
            if batch:
                data["create"] = batch
                batch_created_count = len(batch)
        
        # Dodajemy aktualizacje
        if update_idx < len(updates):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            batch = updates[update_idx:update_idx + remaining]
            if batch:
                data["update"] = batch
                batch_updated_count = len(batch)
        
        # Dodajemy usunięcia
        if delete_idx < len(deletions):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            batch = deletions[delete_idx:delete_idx + remaining]
            if batch:
                data["delete"] = batch
                batch_deleted_count = len(batch)
        
        if not data:
            break
        
        try:
            response = wcapi.post("products/batch", data).json()
            
            # Przetwarzamy utworzone produkty
            created = response.get("create", [])
            for item in created:
                if item.get("error"):
                    log.error(f"Błąd podczas tworzenia produktu (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Utworzono produkt '{item.get('name', 'N/A')}' w WooCommerce (ID: {item.get('id')}).")
            all_created.extend(created)
            create_idx += batch_created_count
            
            # Przetwarzamy zaktualizowane produkty
            updated = response.get("update", [])
            for item in updated:
                if item.get("error"):
                    log.error(f"Błąd podczas aktualizacji produktu (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Zaktualizowano produkt '{item.get('name', 'N/A')}' w WooCommerce (ID: {item.get('id')}).")
            all_updated.extend(updated)
            update_idx += batch_updated_count
            
            # Przetwarzamy usunięte produkty
            deleted = response.get("delete", [])
            for item in deleted:
                if item.get("error"):
                    log.error(f"Błąd podczas usuwania produktu (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Usunięto produkt z WooCommerce (ID: {item.get('id')}).")
            all_deleted.extend(deleted)
            delete_idx += batch_deleted_count
            
            time.sleep(1)  # Krótkie opóźnienie między partiami aby uniknąć limitów API
            
        except HTTPError as http_err:
            log.error(f"Błąd HTTP podczas batchowej synchronizacji produktów: {http_err}")
            status = False
            break
        except Exception as e:
            log.error(f"Błąd podczas batchowej synchronizacji produktów: {e}")
            status = False
            break

    # Wyświetlamy podsumowanie
    successful_created_count = len([i for i in all_created if not i.get("error")])
    successful_updated_count = len([i for i in all_updated if not i.get("error")])
    successful_deleted_count = len([i for i in all_deleted if not i.get("error")])
    stats = []
    if creations:
        stats.append(f"{successful_created_count}/{len(creations)} utworzonych")
    if updates:
        stats.append(f"{successful_updated_count}/{len(updates)} zaktualizowanych")
    if deletions:
        stats.append(f"{successful_deleted_count}/{len(deletions)} usuniętych")
    log.info("Zakończono synchornizacje: " + ", ".join(stats) + " produktów.")

    return status, all_created, all_updated, all_deleted


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
    parser.add_argument(
        "--odtworz",
        dest="full_rebuild",
        action="store_true",
        default=False,
        help="Tworzy wszystkie elementy bez względu na istniejące dane."
    )
    parser.add_argument(
        "--regeneruj",
        dest="regeneruj",
        action="store_true",
        default=False,
        help="Usuwa wszystkie produkty z WooCommerce, resetuje wersję śledzenia i synchronizuje ponownie."
    )
    
    global args
    args = parser.parse_args()
    
    # Wczytanie wersji śledzenia zmian
    load_change_tracking_versions()
    
    # Połączenie z bazą danych i WooCommerce API
    global cursor, conn, wcapi
    try:
        cursor, conn = get_database_connection()
        wcapi = get_woocommerce_api()
    except Exception as e:
        log.error(f"Błąd podczas łączenia się z zasobami: {e}")
        raise

    # Konfiguracja bazy danych do śledzenia zmian jeśli flaga jest ustawiona (--setup)
    if args.setup:
        setup()
        return
    
    # Regeneracja - usuwa wszystkie produkty z WooCommerce i synchronizuje ponownie (--regeneruj)
    if args.regeneruj:
        regenerate()
        return

    # Synchronizacja produktów
    success = sync_products()
    
    # Zapisanie zaktualizowanych wersji śledzenia zmian
    if success: save_change_tracking_versions()

def is_already_enabled_error(error):
    """
    Sprawdza czy błąd oznacza że change tracking jest już włączone.
    """
    msg = str(error).lower()
    return "change tracking" in msg and ("already enabled" in msg or "already been enabled" in msg)

def load_change_tracking_versions():
    """
    Wczytuje wersje śledzenia zmian z pliku JSON.
    """
    global change_tracking_versions
    if os.path.exists(CHANGE_TRACKING_FILE):
        try:
            with open(CHANGE_TRACKING_FILE, 'r', encoding='utf-8') as f:
                change_tracking_versions = json.load(f)
            log.debug(f"Wczytano wersje śledzenia zmian z {CHANGE_TRACKING_FILE}")
        except (json.JSONDecodeError, IOError) as e:
            log.warning(f"Nie udało się wczytać wersji śledzenia zmian: {e}. Tworzenie nowego pliku.")
            change_tracking_versions = {}
    else:
        change_tracking_versions = {}
        log.debug(f"Plik {CHANGE_TRACKING_FILE} nie istnieje. Tworzenie nowego.")


def save_change_tracking_versions():
    """
    Zapisuje wersje śledzenia zmian do pliku JSON.
    """
    try:
        with open(CHANGE_TRACKING_FILE, 'w', encoding='utf-8') as f:
            json.dump(change_tracking_versions, f, indent=2, ensure_ascii=False)
        log.debug(f"Zapisano wersje śledzenia zmian do {CHANGE_TRACKING_FILE}")
    except IOError as e:
        log.error(f"Nie udało się zapisać wersji śledzenia zmian: {e}")


def get_current_change_tracking_version(table_key: str) -> int | None:
    """
    Pobiera aktualną wersję śledzenia zmian dla danej tabeli z bazy danych.
    
    Args:
        table_key: Klucz tabeli w formacie "[db].[schema].[table]"
        
    Returns:
        Aktualna wersja CHANGE_TRACKING_CURRENT_VERSION() lub None w przypadku błędu
    """
    database_name = os.getenv("database_name")
    try:
        # Pobieramy aktualną wersję z bazy (jest to jedna wartość na całą bazę danych)
        cursor.execute('''SELECT CHANGE_TRACKING_CURRENT_VERSION()''')
        row = cursor.fetchone()
        if row:
            return row[0]
    except pyodbc.Error as e:
        log.error(f"Błąd podczas pobierania wersji śledzenia zmian dla {table_key}: {e}")
    return None

def setup():
    """
    Konfigugurje baze danych do synchronizacji.
    Obsługuje przypadki częściowego włączenia śledzenia zmian.
    Tworzy schemat ERPFlow i tabelę WoocommerceIDs jeśli nie istnieją.
    """
    database_name = os.getenv("database_name")
    tracked_tables = ["Towary", "TwrCeny"]
    
    try:
        # Utworzenie schematu ERPFlow jeśli nie istnieje
        try:
            cursor.execute(f'''IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ERPFlow') EXEC('CREATE SCHEMA ERPFlow');''')
            log.debug(f"Utworzono lub schemat 'ERPFlow' już istnieje.")
        except pyodbc.Error as schema_error:
            log.warning(f"Nie udało się utworzyć schematu 'ERPFlow': {schema_error}")
            raise
        
        # Utworzenie tabeli WoocommerceIDs jeśli nie istnieje
        try:
            cursor.execute(f'''
                IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'ERPFlow' AND t.name = 'WoocommerceIDs')
                CREATE TABLE [ERPFlow].[WoocommerceIDs] (
                    Twr_TwrId INT NOT NULL UNIQUE,
                    WC_ID INT NOT NULL PRIMARY KEY,
                    CONSTRAINT FK_WoocommerceIDs_Towary FOREIGN KEY (Twr_TwrId) REFERENCES CDN.Towary(Twr_TwrId)
                );
            ''')
            log.debug(f"Utworzono lub tabela 'WoocommerceIDs' już istnieje.")
        except pyodbc.Error as table_error:
            log.warning(f"Nie udało się utworzyć tabeli 'WoocommerceIDs': {table_error}")
            raise
        
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
        log.info("Konfiguracja bazy danych zakończona pomyślnie.")
    except pyodbc.Error as e:
        log.error(f"Błąd podczas konfiguracji bazy danych: {e}")
        raise

def regenerate():
    """
    Usuwa wszystkie produkty z WooCommerce, które mają swoje ID w tabeli WoocommerceIDs,
    resetuje wersję śledzenia zmian do 0 i uruchamia synchronizację.
    """
    global change_tracking_versions
    
    log.info("Rozpoczynanie regeneracji produktów...")
    
    try:
        # Pobieramy wszystkie ID produktów WooCommerce z tabeli WoocommerceIDs
        cursor.execute('SELECT WC_ID FROM [ERPFlow].[WoocommerceIDs]')
        wc_ids = [row[0] for row in cursor.fetchall()]
        
        if wc_ids:
            log.info(f"Znaleziono {len(wc_ids)} produktów do usunięcia z WooCommerce.")
            
            batch_sync_products(deletions=wc_ids)
            
            # Czyścimy tabelę WoocommerceIDs
            cursor.execute('DELETE FROM [ERPFlow].[WoocommerceIDs]')
            log.info("Wyczyszczono tabelę WoocommerceIDs.")
        else:
            log.info("Brak produktów do usunięcia w tabeli WoocommerceIDs.")
        
        # Resetujemy wersję śledzenia zmian do 0
        database_name = os.getenv("database_name")
        tables = [f"[{database_name}].[CDN].[Towary]", f"[{database_name}].[CDN].[TwrCeny]"]
        
        for table_key in tables:
            change_tracking_versions[table_key] = 0
            log.debug(f"Zresetowano wersję śledzenia zmian dla {table_key} do 0.")
        
        # Zapisujemy zresetowane wersje
        save_change_tracking_versions()
        log.info("Zresetowano wersje śledzenia zmian do 0.")
        
        # Uruchamiamy synchronizację
        log.info("Rozpoczynanie synchronizacji produktów...")
        success = sync_products()
        
        # Zapisujemy zaktualizowane wersje śledzenia zmian
        if success:
            save_change_tracking_versions()
            log.info("Regeneracja zakończona pomyślnie.")
        else:
            log.error("Regeneracja zakończona z błędami.")
            
    except Exception as e:
        log.error(f"Błąd podczas regeneracji: {e}", stack_info=True)


def sync_products() -> bool:
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    Używa Change Tracking do synchronizacji tylko zmienionych rekordów.
    Returns:
        bool: True jeśli synchronizacja zakończyła się sukcesem, False w przeciwnym razie.
    """
    database_name = os.getenv("database_name")
    tables = [f"[{database_name}].[CDN].[Towary]", f"[{database_name}].[CDN].[TwrCeny]"]
    status = True
    
    # Pobieramy aktualne wersje dla tabel (przed synchronizacją)
    current_versions = {}
    for table_key in tables:
        version = get_current_change_tracking_version(table_key)
        if version is None:
            log.error(f"Nie udało się pobrać wersji śledzenia zmian dla {table_key}. Przerywanie synchronizacji.")
            return False
        current_versions[table_key] = version
        last_sync_version = change_tracking_versions.get(table_key, 0)
        log.debug(f"Tabela {table_key}: ostatnia wersja = {last_sync_version}, aktualna = {version}")
    
    try:
        # Jeśli mamy zapisaną wersję, używamy CHANGETABLE do pobrania tylko zmian
        # W przeciwnym razie (pierwsza synchronizacja) pobieramy wszystko
        has_previous_sync = all(
            change_tracking_versions.get(table_key) not in (None, 0)
            for table_key in tables
        )
        
        if has_previous_sync and not args.full_rebuild:
            # Synchronizacja przyrostowa - tylko zmienione rekordy
            log.debug("Wykryto poprzednią synchronizację.")
            last_version = min(change_tracking_versions.get(table_key, 0) for table_key in tables)
            
            query = f'''
                SELECT DISTINCT GREATEST(ct_t.SYS_CHANGE_VERSION, ct_tc.SYS_CHANGE_VERSION) AS SYS_CHANGE_VERSION,
                                t.Twr_TwrId,
                                t.Twr_Nazwa,
                                t.Twr_Opis,
                                tc.TwC_Wartosc,
                                tc.TwC_Zaokraglenie
                FROM [{database_name}].[CDN].[Towary] t
                INNER JOIN [{database_name}].[CDN].[TwrCeny] tc ON t.Twr_TwrId = tc.TwC_TwrID
                LEFT JOIN CHANGETABLE(CHANGES [{database_name}].[CDN].[Towary], {last_version}) ct_t ON t.Twr_TwrId = ct_t.Twr_TwrId
                LEFT JOIN CHANGETABLE(CHANGES [{database_name}].[CDN].[TwrCeny], {last_version}) ct_tc ON tc.TwC_TwrID = ct_tc.TwC_TwCID
                WHERE tc.TwC_Typ = 2
                AND (ct_t.SYS_CHANGE_VERSION IS NOT NULL
                    OR ct_tc.SYS_CHANGE_VERSION IS NOT NULL)
            '''
            cursor.execute(query)
        else:
            # Pełna synchronizacja - wszystkie rekordy
            log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich produktów.")
            query = f'''
                SELECT DISTINCT t.Twr_TwrId, Twr_Nazwa, Twr_Opis, TwC_Wartosc, TwC_Zaokraglenie 
                FROM [{database_name}].[CDN].[Towary] t
                INNER JOIN [{database_name}].[CDN].[TwrCeny] tc ON t.Twr_TwrId = tc.TwC_TwrID
                WHERE tc.TwC_Typ = 2
            '''
            cursor.execute(query)
        
        products = cursor.fetchall()

        products_to_create = {}        
        for product in products:
            regular_price = str(round(round(product.TwC_Wartosc / product.TwC_Zaokraglenie) * product.TwC_Zaokraglenie, 2))
            
            # Pomijamy darmowe towary jeśli flaga nie jest ustawiona
            if float(regular_price) == 0 and not args.obejmuj_darmowe_towary:
                log.warning(f"Pominięto darmowy produkt '{product.Twr_Nazwa}'. Użyj --obejmuj-darmowe-towary, aby zsynchronizować również darmowe towary.")
                continue
            
            product_data = {
                "name": product.Twr_Nazwa,
                "description": product.Twr_Opis,
                "regular_price": regular_price,
                "sku": product.Twr_TwrId,
            }
            products_to_create[product.Twr_TwrId] = product_data
            log.debug(f"Przygotowano produkt do synchronizacji: {product_data}")

        success, created_items, _, _ = batch_sync_products(creations=list(products_to_create.values()))
        
        if not success:
            return False
        
        total_synced = 0
        for idx, item in enumerate(created_items):
            if item.get("id") and not item.get("error"):
                product_data_index = idx  # Indeks w oryginalnej liście produktów
                comarch_id = list(products_to_create.keys())[product_data_index]
                cursor.execute(f'''MERGE [ERPFlow].[WoocommerceIDs] AS target
                    USING (VALUES ({comarch_id}, {item.get("id")})) AS source (Twr_TwrId, WC_ID)
                    ON target.Twr_TwrId = source.Twr_TwrId OR target.WC_ID = source.WC_ID
                    WHEN MATCHED THEN
                        UPDATE SET Twr_TwrId = source.Twr_TwrId, WC_ID = source.WC_ID
                    WHEN NOT MATCHED THEN
                        INSERT (Twr_TwrId, WC_ID) VALUES (source.Twr_TwrId, source.WC_ID);''')

                total_synced += 1
        
        log.info(f"Zakończono synchronizacje produktów. Zsynchronizowano {total_synced}/{len(products_to_create)} produktów.")
        
        # Aktualizujemy wersje śledzenia zmian po udanej synchronizacji
        for table_key, version in current_versions.items():
            change_tracking_versions[table_key] = version
            log.debug(f"Zaktualizowano wersję śledzenia zmian dla {table_key}: {version}")
            
    except Exception as e:
        log.error(f"Błąd podczas synchronizacji produktów: {e}", stack_info=True)
        status = False
    return status
    
    
if __name__ == "__main__":
    main()