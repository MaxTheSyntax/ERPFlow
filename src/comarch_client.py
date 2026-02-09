import connections as con
import os
import pyodbc
import json
import logger as log

# Ścieżka do pliku JSON przechowującego czas synchronizacji
SYNC_STATE_FILE = os.path.join(os.path.dirname(__file__), "sync_state.json")

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


def get_current_timestamp() -> str | None:
    """
    Pobiera aktualny znacznik czasu z bazy danych.
    
    Returns:
        Aktualny timestamp w formacie ISO lub None w przypadku błędu
    """
    try:
        con.cursor.execute('''SELECT SYSUTCDATETIME()''')
        row = con.cursor.fetchone()
        if row:
            if hasattr(row[0], 'isoformat'):
                return row[0].strftime("%Y-%m-%d %H:%M:%S")
            else: str(row[0])
    except pyodbc.Error as e:
        log.error(f"Błąd podczas pobierania aktualnego czasu z bazy: {e}")
    return None


def get_changed_columns(twr_id: int, last_sync: str, current_time: str, force: bool = False) -> dict:
    """
    Pobiera szczegółowe informacje o zmianach w kolumnach dla danego produktu.
    Używa tabel tymczasowych (temporal tables) do porównania stanu sprzed i po synchronizacji.
    
    Args:
        twr_id: ID produktu w tabeli Towary
        last_sync: Znacznik czasu ostatniej synchronizacji (ISO format)
        current_time: Aktualny znacznik czasu (ISO format)
        force: Jeśli True, traktuje produkt jako całkowicie zmieniony jeśli wystąpi błąd podczas pobierania zmian (domyślnie False)
    Returns:
        Słownik z nazwami zmienionych kolumn i ich wartościami (stara_wartosc -> nowa_wartosc)
    """
    database_name = os.getenv("database_name")
    changes = {}
    
    # Jeśli brak last_sync (pierwsza synchronizacja), zwracamy pusty słownik
    if last_sync is None:
        return changes
    
    try:
        # Pobieramy zmiany z tabeli Towary
        # Konwertujemy last_sync na datetime w formacie ISO 8601 (YYYY-MM-DDTHH:MM:SS)
        last_sync_iso = last_sync.replace(' ', 'T') if ' ' in last_sync else last_sync
        con.cursor.execute(f'''
            SELECT 
                t_now.Twr_Nazwa AS nowa_nazwa,
                t_history.Twr_Nazwa AS stara_nazwa,
                t_now.Twr_Opis AS nowy_opis,
                t_history.Twr_Opis AS stary_opis
            FROM [{database_name}].[CDN].[Towary] t_now
            LEFT JOIN [{database_name}].[CDN].[Towary]
                FOR SYSTEM_TIME AS OF ? t_history
                ON t_now.Twr_TwrId = t_history.Twr_TwrId
            WHERE t_now.Twr_TwrId = ?
        ''', (last_sync_iso, twr_id))
        row = con.cursor.fetchone()
        if row:
            if row.stara_nazwa != row.nowa_nazwa:
                changes['Twr_Nazwa'] = {'old': row.stara_nazwa, 'new': row.nowa_nazwa}
            if row.stary_opis != row.nowy_opis:
                changes['Twr_Opis'] = {'old': row.stary_opis, 'new': row.nowy_opis}
        
        # Pobieramy zmiany z tabeli TwrCeny
        con.cursor.execute(f'''
            SELECT 
                tc_now.TwC_Wartosc AS nowa_wartosc,
                tc_history.TwC_Wartosc AS stara_wartosc,
                tc_now.TwC_Zaokraglenie AS nowe_zaokraglenie,
                tc_history.TwC_Zaokraglenie AS stare_zaokraglenie
            FROM [{database_name}].[CDN].[TwrCeny] tc_now
            LEFT JOIN [{database_name}].[CDN].[TwrCeny]
                FOR SYSTEM_TIME AS OF ? tc_history
                ON tc_now.TwC_TwrID = tc_history.TwC_TwrID 
                AND tc_now.TwC_Typ = tc_history.TwC_Typ
            WHERE tc_now.TwC_TwrID = ?
            AND tc_now.TwC_Typ = 2
        ''', (last_sync_iso, twr_id))
        row = con.cursor.fetchone()
        if row:
            old_price = round(row.stara_wartosc / row.stare_zaokraglenie) * row.stare_zaokraglenie if row.stara_wartosc else None
            new_price = round(row.nowa_wartosc / row.nowe_zaokraglenie) * row.nowe_zaokraglenie if row.nowa_wartosc else None
            if old_price != new_price:
                changes['Cena'] = {'old': old_price, 'new': new_price}
                
    except pyodbc.Error as e:
        log.error(f"Nie udało się pobrać szczegółowych zmian dla produktu {twr_id}: {e}")
        if not force:
            raise
        else:
            log.info("Tryb 'force' włączony. Produkt będzie traktowany jako całkowicie zmieniony, ponieważ nie można porównać stanu sprzed i po synchronizacji.")
    
    return changes