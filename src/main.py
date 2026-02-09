import pyodbc
from dotenv import load_dotenv
import argparse
from args import args
import connections as con
import logger as log
import comarch_client as db
import products

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
    
    global args
    args = parser.parse_args()

    log.set_log_level(args.log_level)

    # Inicljalizacja połączeń
    con.initialize()

    # Wczytanie stanu synchronizacji
    db.load_sync_state()

    # Konfiguracja (jeżeli --setup)
    if args.setup:
        setup()
        return
    
    # Regeneracja - usuwa wszystkie produkty z WooCommerce i synchronizuje ponownie (--regeneruj)
    if args.regeneruj:
        products.full_rebuild()
        return
    
    success = True
    # Synchronizacja produktów
    success = products.sync()
    
    # Zapisanie zaktualizowanego stanu synchronizacji
    if success: 
        db.save_sync_state()
    else:
        log.warning("UWAGA: Synchronizacja zakończyła się z błędami. Mogą istnieć produkty w WooCommerce, które nie są poprawnie zapisane w Comarchu. Sprawdź logi powyżej, aby zidentyfikować problemy.")

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
            con.cursor.execute(f'''IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ERPFlow') EXEC('CREATE SCHEMA ERPFlow');''')
            log.debug(f"Utworzono lub schemat 'ERPFlow' już istnieje.")
        except pyodbc.Error as schema_error:
            log.error(f"Nie udało się utworzyć schematu 'ERPFlow': {schema_error}")
            raise
        
        # Utworzenie tabeli WoocommerceIDs jeśli nie istnieje
        try:
            con.cursor.execute(f'''
                IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'WoocommerceIDs' AND schema_id = SCHEMA_ID('ERPFlow'))
                CREATE TABLE [ERPFlow].[WoocommerceIDs] (
                    Twr_TwrId INT PRIMARY KEY,
                    WC_ID INT NOT NULL,
                    LastSynced DATETIME2 DEFAULT GETDATE()
                );
            ''')
            log.debug(f"Utworzono lub tabela 'WoocommerceIDs' już istnieje.")
        except pyodbc.Error as table_error:
            log.error(f"Nie udało się utworzyć tabeli 'WoocommerceIDs': {table_error}")
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
