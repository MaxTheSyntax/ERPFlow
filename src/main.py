import time
import pyodbc
from woocommerce import API
from requests.exceptions import HTTPError
from dotenv import load_dotenv
import os
import argparse
import json
from connections import wcapi, cursor
import logger as log

# Ścieżka do pliku JSON przechowującego czas synchronizacji
SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__), "sync_state.json")

# Zmienne globalne
args = None
conn = None

# slownik timestampu ISO ostaniej synchronizacji
sync_state = {}

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
                    log.debug(f"Utworzono produkt '{item.get('name', 'N/A')}' w WooCommerce (ID: {item.get('id')}).")
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
    log.debug("Zakończono synchornizacje: " + ", ".join(stats) + " produktów.")

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

    # Wczytanie stanu synchronizacji
    load_sync_state()

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
    
    # Zapisanie zaktualizowanego stanu synchronizacji
    if success: 
        save_sync_state()
    else:
        log.warning("UWAGA: Synchronizacja zakończyła się z błędami. Mogą istnieć produkty w WooCommerce, które nie są poprawnie zapisane w Comarchu. Sprawdź logi powyżej, aby zidentyfikować problemy.")

def is_temporal_enabled(table_name: str, schema: str = 'CDN') -> bool:
    """
    Sprawdza czy temporal tables są już włączone dla danej tabeli.
    """
    database_name = os.getenv("database_name")
    try:
        cursor.execute(f'''
            SELECT 1 FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{schema}' AND t.name = '{table_name}'
            AND t.temporal_type IN (1, 2)
        ''')
        return cursor.fetchone() is not None
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


def get_current_timestamp() -> str | None:
    """
    Pobiera aktualny znacznik czasu z bazy danych.
    
    Returns:
        Aktualny timestamp w formacie ISO lub None w przypadku błędu
    """
    try:
        cursor.execute('''SELECT SYSUTCDATETIME()''')
        row = cursor.fetchone()
        if row:
            return row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0])
    except pyodbc.Error as e:
        log.error(f"Błąd podczas pobierania aktualnego czasu z bazy: {e}")
    return None


def get_changed_columns(twr_id: int, last_sync: str, current_time: str) -> dict:
    """
    Pobiera szczegółowe informacje o zmianach w kolumnach dla danego produktu.
    Używa tabel tymczasowych (temporal tables) do porównania stanu sprzed i po synchronizacji.
    
    Args:
        twr_id: ID produktu w tabeli Towary
        last_sync: Znacznik czasu ostatniej synchronizacji (ISO format)
        current_time: Aktualny znacznik czasu (ISO format)
        
    Returns:
        Słownik z nazwami zmienionych kolumn i ich wartościami (stara_wartosc -> nowa_wartosc)
    """
    database_name = os.getenv("database_name")
    changes = {}
    
    try:
        # Pobieramy zmiany z tabeli Towary
        cursor.execute(f'''
            SELECT 
                t_now.Twr_Nazwa AS nowa_nazwa,
                t_history.Twr_Nazwa AS stara_nazwa,
                t_now.Twr_Opis AS nowy_opis,
                t_history.Twr_Opis AS stary_opis
            FROM [{database_name}].[CDN].[Towary] t_now
            LEFT JOIN [{database_name}].[CDN].[Towary]
                FOR SYSTEM_TIME AS OF '{last_sync}' t_history
                ON t_now.Twr_TwrId = t_history.Twr_TwrId
            WHERE t_now.Twr_TwrId = {twr_id}
        ''')
        row = cursor.fetchone()
        if row:
            if row.stara_nazwa != row.nowa_nazwa:
                changes['Twr_Nazwa'] = {'old': row.stara_nazwa, 'new': row.nowa_nazwa}
            if row.stary_opis != row.nowy_opis:
                changes['Twr_Opis'] = {'old': row.stary_opis, 'new': row.nowy_opis}
        
        # Pobieramy zmiany z tabeli TwrCeny
        cursor.execute(f'''
            SELECT 
                tc_now.TwC_Wartosc AS nowa_wartosc,
                tc_history.TwC_Wartosc AS stara_wartosc,
                tc_now.TwC_Zaokraglenie AS nowe_zaokraglenie,
                tc_history.TwC_Zaokraglenie AS stare_zaokraglenie
            FROM [{database_name}].[CDN].[TwrCeny] tc_now
            LEFT JOIN [{database_name}].[CDN].[TwrCeny]
                FOR SYSTEM_TIME AS OF '{last_sync}' tc_history
                ON tc_now.TwC_TwrID = tc_history.TwC_TwrID 
                AND tc_now.TwC_Typ = tc_history.TwC_Typ
            WHERE tc_now.TwC_TwrID = {twr_id}
            AND tc_now.TwC_Typ = 2
        ''')
        row = cursor.fetchone()
        if row:
            old_price = round(row.stara_wartosc / row.stare_zaokraglenie) * row.stare_zaokraglenie if row.stara_wartosc else None
            new_price = round(row.nowa_wartosc / row.nowe_zaokraglenie) * row.nowe_zaokraglenie if row.nowa_wartosc else None
            if old_price != new_price:
                changes['Cena'] = {'old': old_price, 'new': new_price}
                
    except pyodbc.Error as e:
        log.error(f"Nie udało się pobrać szczegółowych zmian dla produktu {twr_id}: {e}")
    
    return changes

def setup():
    """
    Konfiguruje bazę danych do synchronizacji używając tabel tymczasowych (temporal tables).
    Tworzy schemat ERPFlow, tabelę WoocommerceIDs oraz włącza temporal tables tam gdzie trzeba.
    """
    # Lista tabel, dla których chcemy włączyć temporal tables
    tracked_tables = ["Towary", "TwrCeny"]
    
    try:
        # Utworzenie schematu ERPFlow jeśli nie istnieje
        try:
            cursor.execute(f'''IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ERPFlow') EXEC('CREATE SCHEMA ERPFlow');''')
            log.debug(f"Utworzono lub schemat 'ERPFlow' już istnieje.")
        except pyodbc.Error as schema_error:
            log.error(f"Nie udało się utworzyć schematu 'ERPFlow': {schema_error}")
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
            log.error(f"Nie udało się utworzyć tabeli 'WoocommerceIDs': {table_error}")
            raise
        
        # Włączamy temporal tables dla każdej tabeli
        for table in tracked_tables:
            try:
                if is_temporal_enabled(table):
                    log.debug(f"Temporal table jest już włączone dla tabeli '{table}'.")
                    continue
                    
                # Sprawdzamy czy tabela ma już kolumny period
                cursor.execute(f'''
                    SELECT COUNT(*) FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = 'CDN' AND t.name = '{table}'
                    AND c.name IN ('ValidFrom', 'ValidTo')
                ''')
                period_cols = cursor.fetchone()[0]
                
                if period_cols < 2:
                    # Dodajemy kolumny period jeśli nie istnieją
                    cursor.execute(f'''
                        ALTER TABLE [CDN].[{table}]
                        ADD ValidFrom DATETIME2(0) GENERATED ALWAYS AS ROW START HIDDEN DEFAULT SYSUTCDATETIME(),
                            ValidTo DATETIME2(0) GENERATED ALWAYS AS ROW END HIDDEN DEFAULT CONVERT(DATETIME2(0), '9999-12-31 23:59:59'),
                            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo);
                    ''')
                    log.debug(f"Dodano kolumny period dla tabeli '{table}'.")
                
                # Włączamy temporal table
                cursor.execute(f'''
                    ALTER TABLE [CDN].[{table}]
                    SET (SYSTEM_VERSIONING = ON (HISTORY_TABLE = CDN.{table}History, HISTORY_RETENTION_PERIOD = 6 MONTHS));
                ''')
                log.info(f"Włączono temporal table dla tabeli '{table}'.")
                
            except pyodbc.Error as table_error:
                error_msg = str(table_error).lower()
                if "already" in error_msg or "istnieje" in error_msg or "exists" in error_msg:
                    log.warning(f"Temporal table lub jego elementy mogą już istnieć dla tabeli '{table}': {table_error}")
                else:
                    log.error(f"Błąd podczas włączania temporal table dla '{table}': {table_error}")
                    raise
        
        log.info("Konfiguracja bazy danych zakończona pomyślnie.")
    except pyodbc.Error as e:
        log.error(f"Błąd podczas konfiguracji bazy danych: {e}")
        raise

def regenerate():
    """
    Usuwa wszystkie produkty z WooCommerce, które mają swoje ID w tabeli WoocommerceIDs,
    resetuje znacznik czasu synchronizacji i uruchamia pełną synchronizację.
    """
    global sync_state
    
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
        
        # Resetujemy znacznik czasu synchronizacji
        sync_state = {}
        save_sync_state()
        log.info("Zresetowano znacznik czasu synchronizacji.")
        
        # Uruchamiamy synchronizację
        log.info("Rozpoczynanie synchronizacji produktów...")
        success = sync_products()
        
        if success:
            log.info("Regeneracja zakończona pomyślnie.")
        else:
            log.error("Regeneracja zakończona z błędami.")
            
    except Exception as e:
        log.error(f"Błąd podczas regeneracji: {e}", stack_info=True)


def sync_products() -> bool:
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    
    Returns:
        bool: True jeśli synchronizacja zakończyła się sukcesem, False w przeciwnym razie.
    """
    database_name = os.getenv("database_name")
    status = True
    
    # Pobieramy aktualny czas przed synchronizacją
    current_timestamp = get_current_timestamp()
    if current_timestamp is None:
        log.error("Nie udało się pobrać aktualnego czasu z bazy danych. Przerywanie synchronizacji.")
        return False
    
    last_sync_timestamp = sync_state.get('last_sync_timestamp')
    log.debug(f"Ostatnia synchronizacja: {last_sync_timestamp}, aktualny czas: {current_timestamp}")
    
    try:
        # Jeśli mamy zapisaną synchronizację, używamy temporal tables do pobrania tylko zmian
        # W przeciwnym razie (pierwsza synchronizacja) pobieramy wszystko
        has_previous_sync = last_sync_timestamp is not None
        
        # Sprawdź czy temporal tables są włączone jeśli planujemy ich użyć
        if has_previous_sync and not args.full_rebuild:
            if not is_temporal_enabled('Towary') or not is_temporal_enabled('TwrCeny'):
                log.warning("Temporal tables nie są włączone dla tabel Towary/TwrCeny.")
                log.warning("Uruchom aplikację z flagą --setup, aby skonfigurować bazę danych.")
                # return False
                log.warning("Przełączam na pełną synchronizację.")
                args.full_rebuild = True
        
        # Synchronizacja
        if has_previous_sync and not args.full_rebuild:
            log.debug("Wykryto poprzednią synchronizację. Pobieranie zmienionych produktów.")
            
            query = f'''
                SELECT DISTINCT 
                    t.Twr_TwrId,
                    t.Twr_Nazwa,
                    t.Twr_Opis,
                    tc.TwC_Wartosc,
                    tc.TwC_Zaokraglenie
                FROM [{database_name}].[CDN].[Towary] t
                INNER JOIN [{database_name}].[CDN].[TwrCeny] tc 
                    ON t.Twr_TwrId = tc.TwC_TwrID
                WHERE tc.TwC_Typ = 2
                AND (
                    -- Zmiany w tabeli Towary od ostatniej synchronizacji
                    EXISTS (
                        SELECT 1 FROM [{database_name}].[CDN].[Towary] 
                        FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{current_timestamp}' th
                        WHERE th.Twr_TwrId = t.Twr_TwrId
                        AND th.ValidFrom > '{last_sync_timestamp}'
                    )
                    OR
                    -- Zmiany w tabeli TwrCeny od ostatniej synchronizacji
                    EXISTS (
                        SELECT 1 FROM [{database_name}].[CDN].[TwrCeny] 
                        FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{current_timestamp}' tch
                        WHERE tch.TwC_TwrID = tc.TwC_TwrID
                        AND tch.TwC_Typ = 2
                        AND tch.ValidFrom > '{last_sync_timestamp}'
                    )
                )
            '''
            cursor.execute(query)
        else:
            # Pełna synchronizacja - wszystkie rekordy
            if has_previous_sync:
                log.info("Pełna przebudowa: pobieranie wszystkich produktów.")
            else:
                log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich produktów.")
            
            query = f'''
                SELECT DISTINCT t.Twr_TwrId, Twr_Nazwa, Twr_Opis, TwC_Wartosc, TwC_Zaokraglenie 
                FROM [{database_name}].[CDN].[Towary] t
                INNER JOIN [{database_name}].[CDN].[TwrCeny] tc ON t.Twr_TwrId = tc.TwC_TwrID
                WHERE tc.TwC_Typ = 2
            '''
            cursor.execute(query)
        
        products = cursor.fetchall()

        # Sprawdzamy czy jest coś do synchronizacji
        if not products:
            log.info("Brak nowych lub zmienionych produktów do synchronizacji.")
            # Aktualizujemy znacznik czasu nawet gdy nie ma zmian
            sync_state['last_sync_timestamp'] = current_timestamp
            return True

        # Tworzymy mapowanie Comarch ID -> WooCommerce ID
        if args.full_rebuild or args.regeneruj:
            wc_id_map = {}
            log.debug("Pełna przebudowa: reset istniejących mapowań produktów.")
            cursor.execute('DELETE FROM [ERPFlow].[WoocommerceIDs]')
        else:
            cursor.execute('SELECT Twr_TwrId, WC_ID FROM [ERPFlow].[WoocommerceIDs]')
            wc_id_map = {row[0]: row[1] for row in cursor.fetchall()}
            log.debug(f"Pobrano {len(wc_id_map)} istniejących mapowań produktów.")

        products_to_create = []
        products_to_update = []
        
        for product in products:
            regular_price = str(round(round(product.TwC_Wartosc / product.TwC_Zaokraglenie) * product.TwC_Zaokraglenie, 2))
            
            # Pomijamy darmowe towary jeśli flaga nie jest ustawiona
            if float(regular_price) == 0 and not args.obejmuj_darmowe_towary:
                log.warning(f"Pominięto darmowy produkt '{product.Twr_Nazwa}'. Użyj --obejmuj-darmowe-towary, aby zsynchronizować również darmowe towary.")
                continue

            changes = get_changed_columns(product.Twr_TwrId, last_sync_timestamp, current_timestamp)
            product_data = {
                "sku": product.Twr_TwrId
            }
            if changes:
                # Jeśli są zmiany, dodajemy tylko zmienione pola do danych produktu
                if 'Twr_Nazwa' in changes: product_data["name"] = changes['Twr_Nazwa']['new']
                if 'Twr_Opis' in changes: product_data["description"] = changes['Twr_Opis']['new']
                if 'Cena' in changes: product_data["regular_price"] = str(changes['Cena']['new'])
            else:
                # Jeśli nie ma zmian (np. przy pełnej synchronizacji), używamy aktualnych wartości z bazy
                product_data.update({
                    "name": product.Twr_Nazwa,
                    "description": product.Twr_Opis,
                    "regular_price": regular_price
                })
            
            
            # Sprawdzamy czy produkt już istnieje w WooCommerce
            if product.Twr_TwrId in wc_id_map:
                # Produkt istnieje - dodajemy ID do aktualizacji
                product_data["id"] = wc_id_map[product.Twr_TwrId]
                products_to_update.append(product_data)
                
                # Logowanie szczegółowych zmian 
                if has_previous_sync and not args.full_rebuild:
                    if changes:
                        change_details = ", ".join([
                            f"{col}: {info.get('old')} -> {info.get('new')}" 
                            for col, info in changes.items()
                        ])
                        log.debug(f"Zmiany w produkcie ID={product.Twr_TwrId}: {change_details}")
                
                log.debug(f"Przygotowano produkt do aktualizacji: {product_data}")
            else:
                # Nowy produkt
                products_to_create.append(product_data)
                log.debug(f"Przygotowano nowy produkt do utworzenia: {product_data}")

        success, created_items, updated_items, _ = batch_sync_products(
            creations=products_to_create,
            updates=products_to_update
        )
        if not success:
            return False
        
        # Zapisujemy nowo utworzone produkty do tabeli WoocommerceIDs
        for item in created_items:
            if item.get("id") and not item.get("error"):
                # Znajdujemy Comarch ID (Twr_TwrId) na podstawie SKU
                comarch_id = item.get("sku")
                if comarch_id:
                    cursor.execute(f'''MERGE [ERPFlow].[WoocommerceIDs] AS target
                        USING (VALUES ({comarch_id}, {item.get("id")})) AS source (Twr_TwrId, WC_ID)
                        ON target.Twr_TwrId = source.Twr_TwrId OR target.WC_ID = source.WC_ID
                        WHEN MATCHED THEN
                            UPDATE SET Twr_TwrId = source.Twr_TwrId, WC_ID = source.WC_ID
                        WHEN NOT MATCHED THEN
                            INSERT (Twr_TwrId, WC_ID) VALUES (source.Twr_TwrId, source.WC_ID);''')
                    log.debug(f"Zmapowano Comarch ID {comarch_id} na WooCommerce ID {item.get('id')}.")
                else:
                    log.error(f"Nie można znaleźć Comarch ID dla utworzonego produktu WooCommerce ID {item.get('id')}. SKU: {item.get('sku')}")
                    status = False

        
        # Zliczamy zaktualizowane produkty
        total_updated = len([i for i in updated_items if not i.get("error")])
        total_created = len([i for i in created_items if not i.get("error")])
        
        log.info(f"Zakończono synchronizacje produktów. Utworzono {total_created}, zaktualizowano {total_updated} produktów.")
        
        # Aktualizujemy znacznik czasu po udanej synchronizacji
        sync_state['last_sync_timestamp'] = current_timestamp
        log.debug(f"Zaktualizowano znacznik czasu synchronizacji: {current_timestamp}")
            
    except Exception as e:
        log.error(f"Błąd podczas synchronizacji produktów: {e}", stack_info=True)
        status = False
    return status
    
if __name__ == "__main__":
    load_dotenv()
    main()
