import connections as con
import os
import pyodbc
import json
import logger as log

# Ścieżka do pliku JSON przechowującego czas synchronizacji
SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__), "sync_state.json")
sync_state = None
sync_start_timestamp = None

def is_temporal_enabled(table_name: str, schema: str = 'CDN') -> bool:
    """
    Sprawdza czy temporal tables są już włączone dla danej tabeli.
    """
    database_name = os.getenv("database_name")
    try:
        con.cursor.execute(f'''
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND t.name = '{table_name}'
            AND t.temporal_type IN (1, 2)
        ''')
        return con.cursor.fetchone() is not None
    except pyodbc.Error:
        return False

def load_sync_state():
    """
    Wczytuje stan synchronizacji z pliku JSON.
    """
    global sync_state
    if os.path.exists(SYNC_STATE_FILE):
        try:
            with open(SYNC_STATE_FILE, 'r', encoding='utf-8') as f:
                sync_state = json.load(f)
            log.debug(f"Wczytano stan synchronizacji z {SYNC_STATE_FILE}")
        except (json.JSONDecodeError, IOError) as e:
            log.error(f"Nie udało się wczytać stanu synchronizacji: {e}")
            raise
    else:
        sync_state = {}
        log.debug(f"Plik {SYNC_STATE_FILE} nie istnieje. Tworzenie nowego.")


def save_sync_state():
    """
    Zapisuje stan synchronizacji do pliku JSON.
    """
    try:
        with open(SYNC_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(sync_state, f, indent=2, ensure_ascii=False)
        log.debug(f"Zapisano stan synchronizacji do {SYNC_STATE_FILE}")
    except IOError as e:
        log.error(f"Nie udało się zapisać stanu synchronizacji: {e}")


def save_sync_start_timestamp():
    """
    Pobiera aktualny znacznik czasu z bazy danych, i zapisuje go w `sync_start_timestamp`.
    Powinno być tylko używane w main przed rozpoczęciem byle jakiej synchronizacji, 
    by mieć spójny czas rozpoczęcia synchronizacji.
    """
    global sync_start_timestamp
    try:
        con.cursor.execute('''SELECT SYSUTCDATETIME()''')
        row = con.cursor.fetchone()
        if row:
            if hasattr(row[0], 'isoformat'):
                # return row[0].strftime("%Y-%m-%d %H:%M:%S")
                sync_start_timestamp = row[0].strftime("%Y-%m-%d %H:%M:%S")
            else: sync_start_timestamp = str(row[0])
            if not sync_start_timestamp:
                raise ValueError("Pobrany znacznik czasu jest pusty.")
    except pyodbc.Error as e:
        log.error(f"Błąd podczas pobierania aktualnego czasu z bazy: {e}")
        raise



def get_changed_columns(table_name: str, columns: list, record_id: int, id_column: str, last_sync: str | None, current_time: str, force: bool = False) -> dict:
    """
    Pobiera szczegółowe informacje o zmianach w kolumnach dla danego rekordu.
    """
    database_name = os.getenv("database_name")
    changes = {}
    
    # Jeśli brak last_sync (pierwsza synchronizacja), zwracamy pusty słownik
    if last_sync is None:
        return changes
    
    if not columns:
        return changes
    
    try:
        # Konwertujemy last_sync na datetime w formacie ISO 8601 (YYYY-MM-DDTHH:MM:SS)
        last_sync_iso = last_sync.replace(' ', 'T') if ' ' in last_sync else last_sync
        
        # Budujemy zapytanie SELECT z aliasami dla kolumn
        select_columns = []
        for col in columns:
            select_columns.append(f"t_now.{col} AS nowa_{col}")
            select_columns.append(f"t_history.{col} AS stara_{col}")
        
        select_clause = ", ".join(select_columns)
        
        query = f'''
            SELECT {select_clause}
            FROM [{database_name}].[CDN].[{table_name}] t_now
            LEFT JOIN [{database_name}].[CDN].[{table_name}]
                FOR SYSTEM_TIME AS OF ? t_history
                ON t_now.{id_column} = t_history.{id_column}
            WHERE t_now.{id_column} = ?
        '''
        
        con.cursor.execute(query, (last_sync_iso, record_id))
        row = con.cursor.fetchone()
        
        if row:
            # Sprawdzamy każdą kolumnę pod kątem zmian
            for col in columns:
                old_val = getattr(row, f"stara_{col}", None)
                new_val = getattr(row, f"nowa_{col}", None)
                
                if old_val != new_val:
                    changes[col] = {'old': old_val, 'new': new_val}
                
    except pyodbc.Error as e:
        log.error(f"Nie udało się pobrać szczegółowych zmian dla rekordu {record_id} w tabeli {table_name}: {e}")
        if not force:
            raise
        else:
            log.info("Tryb 'force' włączony. Rekord będzie traktowany jako całkowicie zmieniony, ponieważ nie można porównać stanu sprzed i po synchronizacji.")
    
    return changes

def generic_sync(
    entity_name: str,
    fetch_query: str,
    id_mapping_table: str,
    db_id_column: str,
    api_id_column: str,
    data_mapper_func,
    api_batch_func,
    last_sync_timestamp: str | None = None,
    rebuild: bool = False,
    force: bool = False
) -> bool:
    """
    Ogólna funkcja do synchronizacji encji między bazą danych MSSQL a zewnętrznym API.

    Ta funkcja zapewnia ujednolicony sposób synchronizacji różnych encji (produkty, kontrahenci itp.)
    między bazą danych Microsoft SQL Server a zewnętrznymi API (np. WooCommerce, WordPress).
    Obsługuje pobieranie danych, mapowanie ID, przetwarzanie wsadowe oraz utrzymywanie stanu synchronizacji.

    Proces synchronizacji składa się z następujących kroków:
    1. Pobieranie rekordów z bazy danych przy użyciu podanego zapytania
    2. Ładowanie lub resetowanie mapowań ID między bazą danych a API
    3. Mapowanie rekordów bazy do formatu zgodnego z API za pomocą data_mapper_func
    4. Podział rekordów na operacje tworzenia i aktualizacji na podstawie istniejących mapowań
    5. Wysyłanie żądań wsadowych do API przez api_batch_func
    6. Aktualizacja mapowań ID dla nowo utworzonych rekordów
    7. Logowanie wyników synchronizacji

    Args:
        entity_name (str): Nazwa encji czytelna dla człowieka (np. 'produkty', 'kontrahenci')
        fetch_query (str): Zapytanie SQL do pobierania rekordów z bazy danych. Powinno zawierać
            odpowiednie filtrowanie oparte na tabelach temporalnych lub innych mechanizmach wykrywania zmian
        id_mapping_table (str): Nazwa tabeli przechowującej mapowania ID między bazą danych a API
        db_id_column (str): Nazwa kolumny w tabeli mapowań dla ID bazy danych (np. 'twi_id')
        api_id_column (str): Nazwa kolumny w tabeli mapowań dla ID API (np. 'wc_product_id')
        data_mapper_func (callable): Funkcja transformująca wiersz bazy danych do słownika zgodnego z API.
            Sygnatura: (row, last_sync_timestamp, force) -> dict | None
            Powinna zwrócić None, jeśli rekord powinien zostać pominięty
        api_batch_func (callable): Funkcja wysyłająca żądania wsadowe do zewnętrznego API.
            Sygnatura: (creations=list, updates=list, deletions=list) -> (bool, list, list, list)
            Zwraca: (success, created_items, updated_items, deleted_items)
        last_sync_timestamp (str | None, optional): Znacznik czasu ISO ostatniej udanej synchronizacji.
            Używany przez data_mapper_func do wykrywania zmian. Domyślnie None
        rebuild (bool, optional): Jeśli True, czyści wszystkie istniejące mapowania ID i wykonuje pełną przebudowę.
            Przydatne przy pierwszej synchronizacji lub odzyskiwaniu danych. Domyślnie False
        force (bool, optional): Jeśli True, wymusza synchronizację wszystkich rekordów niezależnie od wykrywania zmian.
            Przekazywany do data_mapper_func. Domyślnie False

    Returns:
        bool: True jeśli synchronizacja zakończyła się sukcesem (nawet z częściowymi niepowodzeniami),
            False jeśli wystąpił błąd krytyczny uniemożliwiający kontynuowanie synchronizacji

    Przykład:
        >>> success = generic_sync(
        ...     entity_name="produkty",
        ...     fetch_query="SELECT * FROM CDN.Towary WHERE Twi_Zmiana > '2024-01-01'",
        ...     id_mapping_table="WCProductMapping",
        ...     db_id_column="twi_id",
        ...     api_id_column="wc_product_id",
        ...     data_mapper_func=map_product_to_wc,
        ...     api_batch_func=wc_client.batch_update_products,
        ...     last_sync_timestamp="2024-01-01 00:00:00",
        ...     rebuild=False,
        ...     force=False
        ... )
        >>> print(f"Synchronizacja {'udana' if success else 'nieudana'}")

    Uwaga:
        - Funkcja oczekuje, że połączenie z bazą danych będzie już nawiązane przez moduł `connections`
        - Mapowania ID są przechowywane w schemacie [ERPFlow]
        - Funkcja używa instrukcji MERGE dla wydajnych operacji upsert na tabelach mapowań
        - Elementy API powinny mieć pole 'sku', 'username' lub 'slug' do identyfikacji
        - Niepowodzenia pojedynczych rekordów nie przerywają całego procesu synchronizacji
    """
    database_name = os.getenv("database_name")
    
    # Wykonanie zapytania
    try:
        con.cursor.execute(fetch_query)
        records = con.cursor.fetchall()
    except pyodbc.Error as e:
        log.error(f"Błąd podczas pobierania {entity_name}: {e}")
        return False

    # Sprawdzamy czy jest coś do synchronizacji
    if not records:
        log.info(f"Brak nowych lub zmienionych {entity_name} do synchronizacji.")
        return True

    # Pobieramy mapowanie ID
    wc_id_map = {}
    try:
        if rebuild:
            log.debug(f"Pełna przebudowa: reset istniejących mapowań {entity_name}.")
            con.cursor.execute(f'DELETE FROM [ERPFlow].[{id_mapping_table}]')
        else:
            con.cursor.execute(f'SELECT {db_id_column}, {api_id_column} FROM [ERPFlow].[{id_mapping_table}]')
            wc_id_map = {row[0]: row[1] for row in con.cursor.fetchall()}
            log.debug(f"Pobrano {len(wc_id_map)} istniejących mapowań {entity_name}.")
    except pyodbc.Error as e:
        log.error(f"Błąd podczas operacji na tabeli mapowań {id_mapping_table}: {e}")
        return False

    # Przygotowujemy listy do API
    to_create = []
    to_update = []
    
    # Przechowujemy oryginalne ID z bazy dla każdego elementu wysłanego do API
    # Kluczem będzie unikalny identyfikator w danych (np. sku, username)
    item_map = {} 

    for row in records:
        try:
            db_id = getattr(row, db_id_column)
            data = data_mapper_func(row, last_sync_timestamp, force)
            
            if not data:
                continue

            # Dodajemy identyfikator do danych, aby móc powiązać odpowiedź API z rekordem DB
            # Używamy pola 'sku' jako uniwersalnego nośnika ID (nawet dla userów WP)
            # Jeśli API nie obsługuje 'sku', wrapper api_batch_func powinien to obsłużyć/usunąć przed wysłaniem
            # lub po prostu ignorować.
            
            # W przypadku WP Users 'sku' nie istnieje, ale możemy użyć pola, które API zwróci
            # lub api_batch_func zwróci nam oryginalny obiekt danych w odpowiedzi.
            
            # Przyjmijmy, że data_mapper_func zwraca słownik, który ma pole identyfikujące rekord w API (np. 'sku' dla produktów, 'username' dla userów).
            # Musimy wiedzieć które pole to klucz.
            
            key_field = 'sku' if 'sku' in data else 'username'
            key_value = data.get(key_field)
            
            if not key_value:
                log.warning(f"Brak klucza identyfikującego ({key_field}) w danych dla {entity_name} ID {db_id}. Pomijanie.")
                continue

            item_map[key_value] = db_id

            if db_id in wc_id_map:
                data["id"] = wc_id_map[db_id]
                to_update.append(data)
                log.debug(f"Przygotowano {entity_name} do aktualizacji: {db_id} -> {data['id']}")
            else:
                to_create.append(data)
                log.debug(f"Przygotowano nowy {entity_name} do utworzenia: {db_id}")
                
        except Exception as e:
            log.error(f"Błąd podczas przetwarzania {entity_name} (ID: {getattr(row, db_id_column, 'N/A')}): {e}")
            continue

    if not to_create and not to_update:
        log.info(f"Brak danych do wysłania po przetworzeniu zmian {entity_name}.")
        return True

    # Wykonujemy synchronizację
    # api_batch_func zwraca (success, created_items, updated_items, deleted_items)
    success, created_items, updated_items, _ = api_batch_func(
        creations=to_create,
        updates=to_update
    )
    
    if not success:
        log.error(f"Synchronizacja {entity_name} zakończona błędem API.")
        return False

    # Zapisujemy nowe mapowania
    # Zakładamy, że created_items zawiera pole 'sku' lub 'username' identyfikujące rekord
    for item in created_items:
        item_id = item.get("id")
        
        # Próbujemy znaleźć klucz
        key = item.get('sku') or item.get('username') or item.get('slug')
        
        # Jeśli klucz nie jest wprost w odpowiedzi, a api_batch_func zwraca pełne obiekty API,
        # to może być problem jeśli API nie zwraca wysłanych pól niestandardowych.
        # W takim przypadku item_map może pomóc jeśli iterujemy w tej samej kolejności, ale batch nie gwarantuje kolejności.
        
        # W przypadku WP API create_user zwraca obiekt user z username.
        # W przypadku WC API create_product zwraca produkt z sku.
        
        if not key:
             # Fallback: jeśli mamy item_map i tylko jeden element utworzony... to słabe.
             # W wp_client.py musimy upewnić się, że zwracane obiekty mają to co wysłaliśmy jeśli API tego nie zwraca.
             pass

        if item_id and not item.get("error") and key in item_map:
            db_id = item_map[key]
            try:
                # Merge statement for MSSQL
                query = f'''MERGE [ERPFlow].[{id_mapping_table}] AS target
                    USING (VALUES (?, ?)) AS source ({db_id_column}, {api_id_column})
                    ON target.{db_id_column} = source.{db_id_column} OR target.{api_id_column} = source.{api_id_column}
                    WHEN MATCHED THEN
                        UPDATE SET {db_id_column} = source.{db_id_column}, {api_id_column} = source.{api_id_column}
                    WHEN NOT MATCHED THEN
                        INSERT ({db_id_column}, {api_id_column}) VALUES (source.{db_id_column}, source.{api_id_column});'''
                con.cursor.execute(query, (db_id, item_id))
                log.debug(f"Zmapowano {entity_name} ID {db_id} na API ID {item_id}.")
            except pyodbc.Error as e:
                log.error(f"Błąd zapisu mapowania dla {entity_name} ID {db_id}: {e}")

    total_updated = len([i for i in updated_items if not i.get("error")])
    total_created = len([i for i in created_items if not i.get("error")])

    log.info(f"Zakończono synchronizacje {entity_name}. Utworzono {total_created}, zaktualizowano {total_updated}.")
    
    return True