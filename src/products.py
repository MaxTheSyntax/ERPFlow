import os
import comarch_client as db
import connections as con
import logger as log
import wc_client as wc
import args

def get_incremental_query(database_name, last_sync_timestamp):
    return f'''
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
                FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{db.sync_start_timestamp}' th
                WHERE th.Twr_TwrId = t.Twr_TwrId
                AND th.ValidFrom > '{last_sync_timestamp}'
            )
            OR
            -- Zmiany w tabeli TwrCeny od ostatniej synchronizacji
            EXISTS (
                SELECT 1 FROM [{database_name}].[CDN].[TwrCeny] 
                FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{db.sync_start_timestamp}' tch
                WHERE tch.TwC_TwrID = tc.TwC_TwrID
                AND tch.TwC_Typ = 2
                AND tch.ValidFrom > '{last_sync_timestamp}'
            )
        )
    '''

def get_full_query(database_name):
    return f'''
        SELECT DISTINCT t.Twr_TwrId, Twr_Nazwa, Twr_Opis, TwC_Wartosc, TwC_Zaokraglenie 
        FROM [{database_name}].[CDN].[Towary] t
        INNER JOIN [{database_name}].[CDN].[TwrCeny] tc ON t.Twr_TwrId = tc.TwC_TwrID
        WHERE tc.TwC_Typ = 2
    '''

def map_product_to_wc(product, last_sync_timestamp, force, skip_free=False):
    # Obliczamy cenę regularną z uwzględnieniem zaokrągleń
    regular_price = str(round(round(product.TwC_Wartosc / product.TwC_Zaokraglenie) * product.TwC_Zaokraglenie, 2))

    # Pomijamy darmowe towary jeśli flaga nie jest ustawiona
    if float(regular_price) == 0 and skip_free:
        log.warning(f"Pominięto darmowy produkt '{product.Twr_Nazwa}'. Użyj --obejmuj-darmowe-towary, aby zsynchronizować również darmowe towary.")
        return None

    # Pobieramy szczegółowe zmiany w kolumnach z tabeli Towary
    towary_changes = db.get_changed_columns(
        table_name='Towary',
        columns=['Twr_Nazwa', 'Twr_Opis'],
        record_id=product.Twr_TwrId,
        id_column='Twr_TwrId',
        last_sync=last_sync_timestamp,
        current_time=db.sync_start_timestamp,
        force=force
    )
    
    # Pobieramy zmiany w cenie z tabeli TwrCeny
    ceny_changes = db.get_changed_columns(
        table_name='TwrCeny',
        columns=['TwC_Wartosc', 'TwC_Zaokraglenie'],
        record_id=product.Twr_TwrId,
        id_column='TwC_TwrID',
        last_sync=last_sync_timestamp,
        current_time=db.sync_start_timestamp,
        force=force
    )
    
    # Obliczamy zmianę ceny jeśli są dane z tabeli TwrCeny
    price_change = None
    if ceny_changes:
        old_wartosc = ceny_changes.get('TwC_Wartosc', {}).get('old')
        old_zaokraglenie = ceny_changes.get('TwC_Zaokraglenie', {}).get('old')
        new_wartosc = ceny_changes.get('TwC_Wartosc', {}).get('new')
        new_zaokraglenie = ceny_changes.get('TwC_Zaokraglenie', {}).get('new')
        
        old_price = round(old_wartosc / old_zaokraglenie) * old_zaokraglenie if old_wartosc and old_zaokraglenie else None
        new_price = round(new_wartosc / new_zaokraglenie) * new_zaokraglenie if new_wartosc and new_zaokraglenie else None
        
        if old_price != new_price:
            price_change = {'old': old_price, 'new': new_price}
    
    # Łączymy zmiany
    changes = towary_changes.copy()
    if price_change:
        changes['Cena'] = price_change
    
    product_data = {
        "sku": str(product.Twr_TwrId) # SKU musi być stringiem
    }
    
    if changes:
        # Jeśli są zmiany, dodajemy tylko zmienione pola
        if 'Twr_Nazwa' in changes: product_data["name"] = changes['Twr_Nazwa']['new']
        if 'Twr_Opis' in changes: product_data["description"] = changes['Twr_Opis']['new']
        if 'Cena' in changes: product_data["regular_price"] = str(changes['Cena']['new'])
        
        # Logowanie zmian
        change_details = ", ".join([
            f"{col}: {info.get('old')} -> {info.get('new')}"
            for col, info in changes.items()
        ])
        log.debug(f"Zmiany w produkcie ID={product.Twr_TwrId}: {change_details}")

    else:
        # Jeśli nie ma zmian (np. pełna synchronizacja), używamy aktualnych wartości
        product_data.update({
            "name": product.Twr_Nazwa,
            "description": product.Twr_Opis,
            "regular_price": regular_price
        })
    
    return product_data

def sync(add_all=None, skip_free=None, force=None) -> bool:
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.
    """
    # Argumenty
    if args.args is not None:
        if add_all is None:
            add_all = getattr(args.args, 'full_rebuild', False) or getattr(args.args, 'regeneruj', False)
        if skip_free is None:
            skip_free = not getattr(args.args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args.args, 'force', False)
            
    # Domyślne wartości
    add_all = bool(add_all) if add_all is not None else False
    skip_free = bool(skip_free) if skip_free is not None else False
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
        log.info("Rozpoczynanie synchronizacji przyrostowej produktów...")
    else:
        query = get_full_query(database_name)
        if last_sync_timestamp:
            log.info("Pełna przebudowa: pobieranie wszystkich produktów.")
        else:
            log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich produktów.")

    mapper = lambda row, ls, f: map_product_to_wc(row, ls, f, skip_free=skip_free)

    return db.generic_sync(
        entity_name="produktów",
        fetch_query=query,
        id_mapping_table="TowarIDs",
        db_id_column="Twr_TwrId",
        api_id_column="WC_ID",
        data_mapper_func=mapper,
        api_batch_func=wc.batch_sync_products,
        last_sync_timestamp=last_sync_timestamp,
        rebuild=add_all,
        force=force
    )

def regenerate():
    """
    Usuwa wszystkie produkty z WooCommerce, które mają swoje ID w tabeli TowarIDs,
    resetuje znacznik czasu synchronizacji i tworzy wszystkie produkty.
    """
    log.info("Rozpoczynanie regeneracji produktów...")

    try:
        # Pobieramy wszystkie ID produktów WooCommerce z tabeli TowarIDs
        con.cursor.execute('SELECT WC_ID FROM [ERPFlow].[TowarIDs]')
        wc_ids = [row[0] for row in con.cursor.fetchall()]

        if wc_ids:
            log.info(f"Znaleziono {len(wc_ids)} produktów do usunięcia z WooCommerce.")
            wc.batch_sync_products(deletions=wc_ids)

            # Czyścimy tabelę TowarIDs
            con.cursor.execute('DELETE FROM [ERPFlow].[TowarIDs]')
            log.info("Wyczyszczono tabelę TowarIDs.")
        else:
            log.info("Brak produktów do usunięcia w tabeli TowarIDs.")

        # Resetujemy znacznik czasu synchronizacji
        db.sync_state = {}
        db.save_sync_state()
        log.info("Zresetowano znacznik czasu synchronizacji.")

        # Uruchamiamy synchronizację
        log.info("Rozpoczynanie synchronizacji produktów...")
        success = sync()

        if success:
            log.info("Regeneracja zakończona pomyślnie.")
            # Aktualizujemy timestamp na teraz
            if db.sync_start_timestamp:
                db.sync_state['last_sync_timestamp'] = db.sync_start_timestamp
                db.save_sync_state()
        else:
            log.error("Regeneracja zakończona z błędami.")

    except Exception as e:
        log.error(f"Błąd podczas regeneracji: {e}", stack_info=True)
