import pyodbc
from dotenv import load_dotenv
import argparse
from args import args
import connections as con
import logger as log
import comarch_client as db
import products
import contractors

def main():
    parser = argparse.ArgumentParser(
        description="Synchronizacja produktów między bazą danych MSSQL a WooCommerce."
    )
    # ... (args stay same) ...
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
    parser.add_argument(
        "--wymus",
        dest="force",
        action="store_true",
        default=False,
        help="Wymusza traktowanie wszystkich produktów jako zmienionych, nawet jeśli nie można porównać stanu sprzed i po synchronizacji."
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Ustaw poziom logowania. Domyślnie: INFO."
    )
    parser.add_argument(
        "--tylko-towary",
        dest="only_products",
        action="store_true",
        default=False,
        help="Synchronizuj tylko towary, pomijając kontrahentów."
    )
    parser.add_argument(
        "--tylko-kontrahenci",
        dest="only_contractors",
        action="store_true",
        default=False,
        help="Synchronizuj tylko kontrahentów, pomijając towary."
    )
    
    global args
    args = parser.parse_args()

    # Ustawienie poziomu logowania
    log.set_log_level(args.log_level)

    # Sprawdzenie sprzecznych argumentów
    if args.only_products and args.only_contractors:
        log.error("Nie można jednocześnie synchronizować tylko produktów i tylko kontrahentów.")
        return

    # Inicljalizacja połączeń
    con.initialize()

    # Pobieramy aktualny czas przed synchronizacją i ostatniej synchronizacji
    db.save_sync_start_timestamp()
    db.load_sync_state()

    last_sync_timestamp = db.sync_state.get('last_sync_timestamp')
    log.debug(f"Ostatnia synchronizacja: {last_sync_timestamp}, aktualny czas: {db.sync_start_timestamp}")

    # Sprawdzamy czy temporal tables są włączone
    has_previous_sync = last_sync_timestamp is not None
    add_all = getattr(args, 'full_rebuild', False) or getattr(args, 'regeneruj', False)
    use_incremental = has_previous_sync and not add_all

    if use_incremental:
        if not db.is_temporal_enabled('Towary') or not db.is_temporal_enabled('TwrCeny') or not db.is_temporal_enabled('KntOsoby'):
            log.warning("Temporal tables nie są włączone dla wymaganych tabel.")
            log.warning("Uruchom aplikację z flagą --setup, aby skonfigurować bazę danych.")
            log.warning("Przełączam na pełną synchronizację.")
            use_incremental = False

    # Wczytanie stanu synchronizacji
    db.load_sync_state()

    exclusive = args.only_products or args.only_contractors

    # Konfiguracja (jeżeli --setup)
    if args.setup:
        setup()
        return
    
    # Regeneracja - usuwa wszystkie produkty z WooCommerce i synchronizuje ponownie (--regeneruj)
    if args.regeneruj:
        if not exclusive or args.only_products:
            products.regenerate()
        if not exclusive or args.only_contractors:
            contractors.regenerate()
        return
    
    # Synchronizacja produktów
    if not exclusive or args.only_products:
        products_success = products.sync()
    
    # Synchronizacja kontrahentów
    if not exclusive or args.only_contractors:
        contractors_success = contractors.sync()
    
    # Zapisanie zaktualizowanego stanu synchronizacji jeżeli synchronizacja zakończyła się sukcesem
    if (products_success and contractors_success) or (exclusive and (args.only_products or args.only_contractors)): 

        if db.sync_start_timestamp:
            db.sync_state['last_sync_timestamp'] = db.sync_start_timestamp
        db.save_sync_state()
    else:
        log.warning("UWAGA: Synchronizacja zakończyła się z błędami. Mogą istnieć elementy, które nie są poprawnie zapisane. Sprawdź logi.")

def setup():
    """
    Konfiguruje bazę danych do synchronizacji używając tabel tymczasowych (temporal tables).
    Tworzy schemat ERPFlow, potrzebne tabele oraz włącza temporal tables tam gdzie trzeba.
    """
    # Lista tabel, dla których chcemy włączyć temporal tables
    tracked_tables = ["Towary", "TwrCeny", "KntOsoby"]
    
    try:
        # Utworzenie schematu ERPFlow jeśli nie istnieje
        try:
            con.cursor.execute(f'''IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ERPFlow') EXEC('CREATE SCHEMA ERPFlow');''')
            log.debug(f"Utworzono lub schemat 'ERPFlow' już istnieje.")
        except pyodbc.Error as schema_error:
            log.error(f"Nie udało się utworzyć schematu 'ERPFlow': {schema_error}")
            raise
        
        # Utworzenie tabeli mapowania produktów
        try:
            con.cursor.execute(f'''
                IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'TowarIDs' AND schema_id = SCHEMA_ID('ERPFlow'))
                CREATE TABLE [ERPFlow].[TowarIDs] (
                    Twr_TwrId INT PRIMARY KEY,
                    WC_ID INT NOT NULL,
                    LastSynced DATETIME2 DEFAULT GETDATE()
                );
            ''')
            log.debug(f"Utworzono lub tabela 'TowarIDs' już istnieje.")
        except pyodbc.Error as table_error:
            log.error(f"Nie udało się utworzyć tabeli 'TowarIDs': {table_error}")
            raise

        # Utworzenie tabeli mapowania kontrahentów
        try:
            con.cursor.execute(f'''
                IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'KontrahenciIDs' AND schema_id = SCHEMA_ID('ERPFlow'))
                CREATE TABLE [ERPFlow].[KontrahenciIDs] (
                    KnO_KnOId INT PRIMARY KEY,
                    WC_ID INT NOT NULL,
                    LastSynced DATETIME2 DEFAULT GETDATE()
                );
            ''')
            log.debug(f"Utworzono lub tabela 'KontrahenciIDs' już istnieje.")
        except pyodbc.Error as table_error:
            log.error(f"Nie udało się utworzyć tabeli 'KontrahenciIDs': {table_error}")
            raise
        
        # Włączamy temporal tables dla każdej tabeli
        for table in tracked_tables:
            try:
                if db.is_temporal_enabled(table):
                    log.debug(f"Temporal table jest już włączone dla tabeli '{table}'.")
                    continue
                    
                # Sprawdzamy czy tabela ma już kolumny period
                con.cursor.execute(f'''
                    SELECT COUNT(*) FROM sys.columns c
                    JOIN sys.tables t ON c.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = 'CDN' AND t.name = '{table}'
                    AND c.name IN ('ValidFrom', 'ValidTo')
                ''')
                period_cols = con.cursor.fetchone()[0]
                
                if period_cols < 2:
                    # Dodajemy kolumny period jeśli nie istnieją
                    con.cursor.execute(f'''
                        ALTER TABLE [CDN].[{table}]
                        ADD ValidFrom DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL DEFAULT SYSUTCDATETIME(),
                            ValidTo DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL DEFAULT CONVERT(DATETIME2, '9999-12-31 23:59:59.9999999'),
                            PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo);
                    ''')
                    log.debug(f"Dodano kolumny period dla tabeli '{table}'.")
                
                # Włączamy temporal table
                con.cursor.execute(f'''
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

if __name__ == "__main__":
    load_dotenv()
    main()
