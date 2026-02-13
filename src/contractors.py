import os
import comarch_client as db
import connections as con
import logger as log
import wp_client as wp
import args

def get_incremental_query(database_name, last_sync_timestamp):
    return f'''
        SELECT DISTINCT 
            ko.KnO_KnOId,
            ko.KnO_KntId,
            ko.KnO_Nazwisko,
            ko.KnO_Email
        FROM [{database_name}].[CDN].[KntOsoby] ko
        WHERE 
            -- Zmiany w KntOsoby od ostatniej synchronizacji
            EXISTS (
                SELECT 1 
                FROM [{database_name}].[CDN].[KntOsoby]
                FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{db.sync_start_timestamp}' AS kh
                WHERE kh.KnO_KnOId = ko.KnO_KnOId
                AND kh.ValidFrom > '{last_sync_timestamp}'
            )
    '''

def get_full_query(database_name):
    return f'''
        SELECT DISTINCT 
            ko.KnO_KnOId,
            ko.KnO_KntId,
            ko.KnO_Nazwisko,
            ko.KnO_Email
        FROM [{database_name}].[CDN].[KntOsoby] ko
    '''

def map_contractor_to_wp(contractor, last_sync_timestamp, force):    
    # Tworzymy username
    name = contractor.KnO_Nazwisko.split()
    first_name = name[0]
    username = first_name[:3]
    if len(name) > 1: 
        last_name = name[-1].lower()
        username += last_name[:3].lower()
    username += str(contractor.KnO_KnOId)

    # Pobieramy email
    email = getattr(contractor, 'KnO_Email', '')
    if not email:
        log.warning(f"Brak emaila dla kontrahenta {contractor.KnO_KnOId}. Pomijanie.")
        raise ValueError(f"Brak emaila dla kontrahenta {contractor.KnO_KnOId}")
    business_id = contractor.KnO_KntId

    # Sprawdzamy zmiany
    changes = db.get_changed_columns(
        table_name='KntOsoby',
        columns=['KnO_Nazwisko', 'KnO_Email'],
        record_id=contractor.KnO_KnOId,
        id_column='KnO_KnOId',
        last_sync=last_sync_timestamp,
        current_time=db.sync_start_timestamp,
        force=force
    )

    # Przygotowujemy pola imienia i nazwiska
    first_name_field = name[0] if name else ""
    last_name_field = name[-1] if len(name) > 1 else ""

    data = {
        "username": username,
        "email": email,
        "first_name": first_name_field,
        "last_name": last_name_field,
        "roles": ["customer"],
        "erp_business": str(business_id)
    }

    if changes:
        # Aktualizacja tylko zmienionych pól - username nie jest wysyłany bo jest read-only
        update_data = {}

        if 'KnO_Nazwisko' in changes:
            new_full_name = changes['KnO_Nazwisko']['new'].split()
            update_data['first_name'] = new_full_name[0] if new_full_name else ""
            update_data['last_name'] = new_full_name[-1] if len(new_full_name) > 1 else ""
            log.warning(f"Zmieniono nazwisko kontrahenta ID {contractor.KnO_KnOId} z '{changes['KnO_Nazwisko']['old']}' na '{changes['KnO_Nazwisko']['new']}', ale nazwa użytkownika '{username}' nie może być zmieniona w WordPress")
        if 'KnO_Email' in changes: update_data['email'] = changes['KnO_Email']['new']

        return update_data

    return data

def sync(add_all=None, force=None) -> bool:
    """
    Synchronizuje kontrahentów między bazą danych MSSQL a WordPress.
    """
    # Argumenty
    if args.args is not None:
        if add_all is None:
            add_all = getattr(args.args, 'full_rebuild', False) or getattr(args.args, 'regeneruj', False)
        if force is None:
            force = getattr(args.args, 'force', False)
            
    add_all = bool(add_all) if add_all is not None else False
    force = bool(force) if force is not None else False

    database_name = os.getenv("database_name")
    if not database_name:
        log.error("Nie znaleziono nazwy bazy danych w zmiennych środowiskowych.")
        return False

    last_sync_timestamp = db.sync_state.get('last_sync_timestamp')
    has_previous_sync = last_sync_timestamp is not None
    use_incremental = has_previous_sync and not add_all

    if use_incremental:
        query = get_incremental_query(database_name, last_sync_timestamp)
        log.info("Rozpoczynanie synchronizacji przyrostowej kontrahentów...")
    else:
        query = get_full_query(database_name)
        if last_sync_timestamp:
            log.info("Pełna przebudowa: pobieranie wszystkich kontrahentów.")
        else:
            log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich kontrahentów.")

    return db.generic_sync(
        entity_name="kontrahentów",
        fetch_query=query,
        id_mapping_table="KontrahenciIDs",
        db_id_column="KnO_KnOId",
        api_id_column="WC_ID", # Używamy tej samej nazwy kolumny WC_ID w tabeli, choć to WP User ID
        data_mapper_func=map_contractor_to_wp,
        api_batch_func=wp.batch_sync_users,
        last_sync_timestamp=last_sync_timestamp,
        rebuild=add_all,
        force=force
    )

def regenerate():
    """
    Usuwa wszystkich zsynchronizowanych kontrahentów z WordPress i synchronizuje ponownie.
    """
    log.info("Rozpoczynanie regeneracji kontrahentów...")
    
    try:
        # Pobieramy wszystkie ID z tabeli KontrahenciIDs
        con.cursor.execute('SELECT WC_ID FROM [ERPFlow].[KontrahenciIDs]')
        ids = [row[0] for row in con.cursor.fetchall()]

        if ids:
            log.info(f"Znaleziono {len(ids)} kontrahentów do usunięcia z WordPress.")
            # Używamy wp_client do usuwania
            wp.batch_sync_users(deletions=ids)

            # Czyścimy tabelę
            con.cursor.execute('DELETE FROM [ERPFlow].[KontrahenciIDs]')
            log.info("Wyczyszczono tabelę KontrahenciIDs.")
        else:
            log.info("Brak kontrahentów do usunięcia w tabeli KontrahenciIDs.")

        # Resetujemy stan
        db.sync_state = {}
        db.save_sync_state()
        log.info("Zresetowano znacznik czasu synchronizacji.")

        # Synchronizacja
        log.info("Rozpoczynanie synchronizacji kontrahentów...")
        success = sync()

        if success:
            log.info("Regeneracja kontrahentów zakończona pomyślnie.")
            # Aktualizujemy timestamp na teraz, by kolejne uruchomienie było przyrostowe
            if db.sync_start_timestamp:
                db.sync_state['last_sync_timestamp'] = db.sync_start_timestamp
                db.save_sync_state()
        else:
            log.error("Regeneracja kontrahentów zakończona z błędami.")

    except Exception as e:
        log.error(f"Błąd podczas regeneracji kontrahentów: {e}", stack_info=True)
