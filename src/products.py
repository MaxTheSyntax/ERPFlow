import os
import comarch_client as db
import connections as con
import logger as log
import wc_client as wc
from args import args

def sync(add_all=None, skip_free=None, force=None) -> bool:
    """
    Synchronizuje produkty między bazą danych MSSQL a WooCommerce.

    Returns:
        bool: True jeśli synchronizacja zakończyła się sukcesem, False w przeciwnym razie.
    """
    # Argumenty
    if args is not None:
        if add_all is None:
            add_all = getattr(args, 'full_rebuild', False) or getattr(args, 'regeneruj', False)
        if skip_free is None:
            skip_free = not getattr(args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args, 'force', False)

    database_name = os.getenv("database_name")
    if not database_name:
        log.error("Nie znaleziono nazwy bazy danych w zmiennych środowiskowych.")
        return False

    status = True

    # Pobieramy aktualny czas przed synchronizacją
    current_timestamp = db.get_current_timestamp()
    if current_timestamp is None:
        log.error("Nie udało się pobrać aktualnego czasu z bazy danych. Przerywanie synchronizacji.")
        return False

    last_sync_timestamp = db.sync_state.get('last_sync_timestamp')
    log.debug(f"Ostatnia synchronizacja: {last_sync_timestamp}, aktualny czas: {current_timestamp}")

    try:
        # Sprawdzamy czy temporal tables są włączone
        has_previous_sync = last_sync_timestamp is not None
        use_incremental = has_previous_sync and not add_all

        if use_incremental:
            if not db.is_temporal_enabled('Towary') or not db.is_temporal_enabled('TwrCeny'):
                log.warning("Temporal tables nie są włączone dla tabel Towary/TwrCeny.")
                log.warning("Uruchom aplikację z flagą --setup, aby skonfigurować bazę danych.")
                log.warning("Przełączam na pełną synchronizację.")
                use_incremental = False

        # Wywołujemy odpowiedni tryb synchronizacji
        if use_incremental and isinstance(last_sync_timestamp, str):
            status = incremental_sync(database_name, last_sync_timestamp, current_timestamp)
        else:
            status = full_sync(database_name, current_timestamp)

    except Exception as e:
        log.error(f"Błąd podczas synchronizacji produktów: {e}", stack_info=True)
        status = False
    return status


def full_sync(database_name: str, current_timestamp: str, skip_free=None, force=None) -> bool:
    """
    Tworzy *(nie usuwa)* wszystkie produkty w WooCommerce na podstawie bazy danych, bez względu na poprzedni stan synchronizacji.

    Args:
        database_name (str): Nazwa bazy danych.
        current_timestamp (str): Aktualny znacznik czasu.

    Returns:
        bool: True jeśli sukces, False w przeciwnym razie.
    """
    # Argumenty
    if args is not None:
        if skip_free is None:
            skip_free = not getattr(args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args, 'force', False)

    if db.sync_state.get('last_sync_timestamp'):
        log.info("Pełna przebudowa: pobieranie wszystkich produktów.")
    else:
        log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich produktów.")

    query = f'''
        SELECT DISTINCT t.Twr_TwrId, Twr_Nazwa, Twr_Opis, TwC_Wartosc, TwC_Zaokraglenie 
        FROM [{database_name}].[CDN].[Towary] t
        INNER JOIN [{database_name}].[CDN].[TwrCeny] tc ON t.Twr_TwrId = tc.TwC_TwrID
        WHERE tc.TwC_Typ = 2
    '''
    con.cursor.execute(query)
    products = con.cursor.fetchall()

    return process_and_apply_sync(products, None, current_timestamp)


def incremental_sync(database_name: str, last_sync_timestamp: str, current_timestamp: str, skip_free=None, force=None) -> bool:
    """
    Wykonuje synchronizację przyrostową (tylko zmienione produkty).

    Args:
        database_name (str): Nazwa bazy danych.
        last_sync_timestamp (str): Znacznik czasu ostatniej synchronizacji.
        current_timestamp (str): Aktualny znacznik czasu.

    Returns:
        bool: True jeśli sukces, False w przeciwnym razie.
    """
    # Argumenty
    if args is not None:
        if skip_free is None:
            skip_free = not getattr(args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args, 'force', False)

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
    con.cursor.execute(query)
    products = con.cursor.fetchall()

    return process_and_apply_sync(products, last_sync_timestamp, current_timestamp)


def process_and_apply_sync(products, last_sync_timestamp, current_timestamp, add_all=None, skip_free=None, force=None) -> bool:
    """
    Przetwarza listę produktów i wysyła je do WooCommerce.

    Args:
        products (list): Lista produktów pobrana z bazy.
        last_sync_timestamp (str): Znacznik czasu ostatniej synchronizacji.
        current_timestamp (str): Aktualny znacznik czasu.

    Returns:
        bool: True jeśli sukces, False w przeciwnym razie.
    """
    # Argumenty
    if args is not None:
        if add_all is None:
            add_all = getattr(args, 'full_rebuild', False) or getattr(args, 'regeneruj', False)
        if skip_free is None:
            skip_free = not getattr(args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args, 'force', False)
    
    # Sprawdzamy czy jest coś do synchronizacji
    if not products:
        log.info("Brak nowych lub zmienionych produktów do synchronizacji.")
        # Aktualizujemy znacznik czasu nawet gdy nie ma zmian
        db.sync_state['last_sync_timestamp'] = current_timestamp
        db.save_sync_state()
        return True

    # Pobieramy mapowanie Comarch ID -> WooCommerce ID
    if add_all:
        wc_id_map = {}
        log.debug("Pełna przebudowa: reset istniejących mapowań produktów.")
        con.cursor.execute('DELETE FROM [ERPFlow].[WoocommerceIDs]')
    else:
        con.cursor.execute('SELECT Twr_TwrId, WC_ID FROM [ERPFlow].[WoocommerceIDs]')
        wc_id_map = {row[0]: row[1] for row in con.cursor.fetchall()}
        log.debug(f"Pobrano {len(wc_id_map)} istniejących mapowań produktów.")

    # Przygotowujemy listy produktów do utworzenia i aktualizacji
    products_to_create = []
    products_to_update = []

    for product in products:
        # Obliczamy cenę regularną z uwzględnieniem zaokrągleń
        regular_price = str(round(round(product.TwC_Wartosc / product.TwC_Zaokraglenie) * product.TwC_Zaokraglenie, 2))

        # Pomijamy darmowe towary jeśli flaga nie jest ustawiona
        if float(regular_price) == 0 and skip_free:
            log.warning(f"Pominięto darmowy produkt '{product.Twr_Nazwa}'. Użyj --obejmuj-darmowe-towary, aby zsynchronizować również darmowe towary.")
            continue

        # Pobieramy szczegółowe zmiany w kolumnach
        changes = db.get_changed_columns(product.Twr_TwrId, last_sync_timestamp, current_timestamp, force=force)
        product_data = {
            "sku": product.Twr_TwrId
        }
        
        if changes:
            # Jeśli są zmiany, dodajemy tylko zmienione pola
            if 'Twr_Nazwa' in changes: product_data["name"] = changes['Twr_Nazwa']['new']
            if 'Twr_Opis' in changes: product_data["description"] = changes['Twr_Opis']['new']
            if 'Cena' in changes: product_data["regular_price"] = str(changes['Cena']['new'])
        else:
            # Jeśli nie ma zmian (np. pełna synchronizacja), używamy aktualnych wartości
            product_data.update({
                "name": product.Twr_Nazwa,
                "description": product.Twr_Opis,
                "regular_price": regular_price
            })


        # Sprawdzamy czy produkt już istnieje w WooCommerce
        if product.Twr_TwrId in wc_id_map:
            # Produkt istnieje - przygotowujemy do aktualizacji
            product_data["id"] = wc_id_map[product.Twr_TwrId]
            products_to_update.append(product_data)

            # Logujemy szczegóły zmian jeśli to synchronizacja przyrostowa
            if last_sync_timestamp and not add_all:
                if changes:
                    change_details = ", ".join([
                        f"{col}: {info.get('old')} -> {info.get('new')}"
                        for col, info in changes.items()
                    ])
                    log.debug(f"Zmiany w produkcie ID={product.Twr_TwrId}: {change_details}")

            log.debug(f"Przygotowano produkt do aktualizacji: {product_data}")
        else:
            # Nowy produkt - przygotowujemy do utworzenia
            products_to_create.append(product_data)
            log.debug(f"Przygotowano nowy produkt do utworzenia: {product_data}")

    # Wykonujemy synchronizację batchową
    success, created_items, updated_items, _ = wc.batch_sync_products(
        creations=products_to_create,
        updates=products_to_update
    )
    if not success:
        return False

    # Zapisujemy nowo utworzone mapowania produktów
    for item in created_items:
        if item.get("id") and not item.get("error"):
            comarch_id = item.get("sku")
            if comarch_id:
                con.cursor.execute(f'''MERGE [ERPFlow].[WoocommerceIDs] AS target
                    USING (VALUES ({comarch_id}, {item.get("id")})) AS source (Twr_TwrId, WC_ID)
                    ON target.Twr_TwrId = source.Twr_TwrId OR target.WC_ID = source.WC_ID
                    WHEN MATCHED THEN
                        UPDATE SET Twr_TwrId = source.Twr_TwrId, WC_ID = source.WC_ID
                    WHEN NOT MATCHED THEN
                        INSERT (Twr_TwrId, WC_ID) VALUES (source.Twr_TwrId, source.WC_ID);''')
                log.debug(f"Zmapowano Comarch ID {comarch_id} na WooCommerce ID {item.get('id')}.")
            else:
                log.error(f"Nie można znaleźć Comarch ID dla utworzonego produktu WooCommerce ID {item.get('id')}. SKU: {item.get('sku')}")

    # Zliczamy zaktualizowane produkty
    total_updated = len([i for i in updated_items if not i.get("error")])
    total_created = len([i for i in created_items if not i.get("error")])

    log.info(f"Zakończono synchronizacje produktów. Utworzono {total_created}, zaktualizowano {total_updated} produktów.")

    # Aktualizujemy znacznik czasu po udanej synchronizacji
    db.sync_state['last_sync_timestamp'] = current_timestamp
    db.save_sync_state()
    log.debug(f"Zaktualizowano znacznik czasu synchronizacji: {current_timestamp}")
    
    return True

def full_rebuild():
    """
    Usuwa wszystkie produkty z WooCommerce, które mają swoje ID w tabeli WoocommerceIDs,
    resetuje znacznik czasu synchronizacji i tworzy wszystkie produkty.
    """
    log.info("Rozpoczynanie regeneracji produktów...")

    try:
        # Pobieramy wszystkie ID produktów WooCommerce z tabeli WoocommerceIDs
        con.cursor.execute('SELECT WC_ID FROM [ERPFlow].[WoocommerceIDs]')
        wc_ids = [row[0] for row in con.cursor.fetchall()]

        if wc_ids:
            log.info(f"Znaleziono {len(wc_ids)} produktów do usunięcia z WooCommerce.")

            wc.batch_sync_products(deletions=wc_ids)

            # Czyścimy tabelę WoocommerceIDs
            con.cursor.execute('DELETE FROM [ERPFlow].[WoocommerceIDs]')
            log.info("Wyczyszczono tabelę WoocommerceIDs.")
        else:
            log.info("Brak produktów do usunięcia w tabeli WoocommerceIDs.")

        # Resetujemy znacznik czasu synchronizacji
        db.sync_state = {}
        db.save_sync_state()
        log.info("Zresetowano znacznik czasu synchronizacji.")

        # Uruchamiamy synchronizację
        log.info("Rozpoczynanie synchronizacji produktów...")
        success = sync()

        if success:
            log.info("Regeneracja zakończona pomyślnie.")
        else:
            log.error("Regeneracja zakończona z błędami.")

    except Exception as e:
        log.error(f"Błąd podczas regeneracji: {e}", stack_info=True)