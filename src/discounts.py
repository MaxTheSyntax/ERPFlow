import args
import os
import logger as log
import comarch_client as db
from requests.exceptions import HTTPError
import time
import efwp_client as efwp
from decimal import *
getcontext().prec = 2

def get_incremental_query(database_name, last_sync_timestamp):
    return f'''
        SELECT DISTINCT 
            r.Rab_RabId,
            r.Rab_Typ,
            r.Rab_TwrId,
            ti.WC_ID,
			r.Rab_PodmiotId,
			r.Rab_Rabat,
			r.Rab_Cena,
			r.Rab_DataOd,
			r.Rab_DataDo
        FROM [{database_name}].[CDN].[Rabaty] r
        INNER JOIN [{database_name}].[ERPFlow].[TowarIDs] ti
	        ON r.Rab_TwrId = ti.Twr_TwrId
        WHERE r.Rab_PodmiotTyp = 1
        --AND r.Rab_TypCenyNB = 2
        AND (
            -- Zmiany w tabeli Rabaty od ostatniej synchronizacji
            EXISTS (
                SELECT 1 FROM [{database_name}].[CDN].[Rabaty] 
                FOR SYSTEM_TIME BETWEEN '{last_sync_timestamp}' AND '{db.sync_start_timestamp}' rh
                WHERE rh.Rab_RabId = r.Rab_RabId
                AND rh.ValidFrom > '{last_sync_timestamp}'
            )
        )
    '''

def get_full_query(database_name):
    return f'''
        SELECT DISTINCT 
            r.Rab_RabId,
            r.Rab_Typ,
            r.Rab_TwrId,
            ti.WC_ID,
			r.Rab_PodmiotId,
			r.Rab_Rabat,
			r.Rab_Cena,
			r.Rab_DataOd,
			r.Rab_DataDo
        FROM [{database_name}].[CDN].[Rabaty] r
        INNER JOIN [{database_name}].[ERPFlow].[TowarIDs] ti
	        ON r.Rab_TwrId = ti.Twr_TwrId
        WHERE r.Rab_PodmiotTyp = 1
        --AND r.Rab_TypCenyNB = 2
    '''

def sync(add_all=None, skip_free=None, force=None) -> bool:
    """
    Synchronizuje zniżki między bazą danych MSSQL a WooCommerce, uwzględniając zniżki dla kontrahentów.
    
    :param skip_free: Flaga określająca, czy pomijać darmowe towary (cena 0). Domyślnie True.
    :param force: Flaga wymuszająca synchronizację, nawet jeśli nie wykryto zmian.

    :return: True jeżeli wszystko zostało zsynchronizowane, False jeżeli nastąpiły błędy.
    """
    # Argumenty
    if args.args is not None:
        if add_all is None:
            add_all = getattr(args.args, 'full_rebuild', False) or getattr(args.args, 'regeneruj', False)
            log.debug(f"add_all: {add_all}, full_rebuild: {getattr(args.args, 'full_rebuild', False)}, regeneruj: {getattr(args.args, 'regeneruj', False)}")
        if skip_free is None:
            skip_free = not getattr(args.args, 'obejmuj_darmowe_towary', False)
        if force is None:
            force = getattr(args.args, 'force', False)
            
    database_name = os.getenv("database_name")
    if not database_name:
        log.error("Nie znaleziono nazwy bazy danych w zmiennych środowiskowych.")
        return False
    
    last_sync_timestamp = db.sync_state.get('last_sync_timestamp')
    has_previous_sync = last_sync_timestamp is not None
    use_incremental = has_previous_sync and not add_all

    if use_incremental:
        query = get_incremental_query(database_name, last_sync_timestamp)
        log.info("Rozpoczynanie synchronizacji przyrostowej zniżek...")
    else: 
        query = get_full_query(database_name)
        if last_sync_timestamp:
            log.info("Pełna przebudowa: pobieranie wszystkich zniżek.")
        else:
            log.info("Brak poprzedniej synchronizacji. Pobieranie wszystkich zniżek.")

    return db.generic_sync(
        entity_name="zniżek",
        fetch_query=query,
        data_mapper_func=lambda row, ls, f: map_discount_to_efwp(row, ls, f, skip_free=skip_free),
        api_batch_func=batch_sync_discounts,
        db_id_column="Rab_RabId",
        last_sync_timestamp=last_sync_timestamp,
        rebuild=add_all,
        force=force
    )

def map_discount_to_efwp(discount, last_sync_timestamp, force, skip_free=False):
    """
    Mapuje dane zniżki kontrahenta z bazy danych MSSQL do formatu oczekiwanego przez WooCommerce.
    
    :param discount: Obiekt reprezentujący zniżkę kontrahenta z bazy danych MSSQL.
    :param last_sync_timestamp: Znacznik czasu ostatniej synchronizacji, używany do określenia, które kolumny uległy zmianie.
    :param force: Flaga wymuszająca synchronizację, nawet jeśli nie wykryto zmian.
    :param skip_free: Flaga określająca, czy pomijać darmowe towary (cena 0). Domyślnie True.
    """

    # 1 - procentowa, 2 - kwotowa
    discount_type = 1 if discount.Rab_Typ < 10 else 2

    contractor = None
    match discount.Rab_Typ:
        case 7 | 8 | 11:
            contractor = -1  # Ogólna zniżka dla wszystkich kontrahentów
        case 1 | 3 | 4 | 12:
            raise NotImplementedError(f"Program nie obsługuje zniżek dla grup kontrahentów. (Rab_Typ: {discount.Rab_Typ})")
        case 2 | 5 | 6 | 13:
            contractor = discount.Rab_PodmiotId  # Zniżka przypisana do konkretnego kontrahenta
        case _:
            raise ValueError(f"Nieznany typ zniżki: {discount.Rab_Typ}")   
        
    product = None
    match discount.Rab_Typ:
        case 1 | 2:
            product = -1  # Ogólna zniżka dla wszystkich towarów
        case 3 | 5 | 7:
            raise NotImplementedError(f"Program nie obsługuje zniżek dla grup towarów. (Rab_Typ: {discount.Rab_Typ})")
        case 4 | 6 | 8 | 11 | 12 | 13:
            product = discount.WC_ID  # Zniżka przypisana do konkretnego towaru
        case _:
            raise ValueError(f"Nieznany typ zniżki: {discount.Rab_Typ}")
        
    if discount_type == 1: # procentowy
        price = Decimal(discount.Rab_Rabat) * Decimal(0.01)
    else: # stały
        price = Decimal(discount.Rab_Rabat)
        
    return {
        "sku": f"DISC_{discount.Rab_RabId}",
        "business_id": contractor, 
        "product_id": product, 
        "discount_type": discount_type,
        "price": str(price)
    }

def batch_sync_discounts(creations: list[dict] = None, updates: list[dict] = None, deletions: list[int] = None) -> tuple[bool, list[dict], list[dict], list[dict]]:
    """
    Wysyła batchowe żądania do WooCommerce API dla tworzenia, aktualizacji i usuwania zniżek kontrahentów.
    Wszystkie trzy operacje mogą być wykonane w jednym żądaniu batch.
    
    :param creations: Lista słowników z danymi zniżek do utworzenia.
    :param updates: Lista słowników z danymi zniżek do zaktualizowania (musi zawierać 'id').
    :param deletions: Lista ID zniżek WooCommerce do usunięcia.

    :return: Tuple (success, created_items, updated_items, deleted_items) gdzie:\n
        - success: True jeśli wszystkie operacje zakończyły się sukcesem, False w przeciwnym razie
        - created_items: Lista utworzonych zniżek z odpowiedzi API
        - updated_items: Lista zaktualizowanych zniżek z odpowiedzi API
        - deleted_items: Lista usuniętych zniżek z odpowiedzią API
    """
    creations = creations or []
    updates = updates or []
    deletions = deletions or []
    
    if not creations and not updates and not deletions:
        log.debug("Brak danych do synchronizacji z WooCommerce.")
        return True, [], [], []
    
    batch_size = 100  # Aby nie wysyłać jednego ogromnego żądania do WooCommerce, dzielimy na partie po 100 zniżek
    status = True
    all_created = []
    all_updated = []
    all_deleted = []

    log.debug(f"Rozpoczynanie operacji w WooCommerce: {len(creations)} utworzeń, {len(updates)} aktualizacji, {len(deletions)} usunięć.")
    
    # Indeksy do śledzenia progressu w każdej liście
    create_idx = 0
    update_idx = 0
    delete_idx = 0
    
    while create_idx < len(creations) or update_idx < len(updates) or delete_idx < len(deletions):
        # Budujemy batch
        creations_data = list()
        updates_data = list()
        deletions_data = list()

        batch_created_count = 0
        batch_updated_count = 0
        batch_deleted_count = 0
        
        # Dodajemy tworzenia
        if create_idx < len(creations):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            creations_data = creations[create_idx:create_idx + remaining]
            batch_created_count = len(creations_data)
            
        
        # Dodajemy aktualizacje
        if update_idx < len(updates):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            updates_data = updates[update_idx:update_idx + remaining]
            batch_updated_count = len(updates_data)
        
        # Dodajemy usunięcia
        if delete_idx < len(deletions):
            remaining = batch_size - (batch_created_count + batch_updated_count + batch_deleted_count)
            deletions_data = deletions[delete_idx:delete_idx + remaining]
            batch_deleted_count = len(deletions_data)
        
        if not creations_data and not updates_data and not deletions_data:
            break
        
        try:
            response = efwp.batch_prices(upsert=creations_data, update=updates_data, delete=deletions_data)
            
            # Przetwarzamy utworzone zniżki
            created = response.get("upsert", [])
            for item in created:
                if item.get("error"):
                    log.error(f"Błąd podczas tworzenia zniżki (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Utworzono zniżke '{item.get('name', 'N/A')}' w WooCommerce (ID: {item.get('id')}).")
            all_created.extend(created)
            create_idx += batch_created_count
            
            # Przetwarzamy zaktualizowane zniżki
            updated = response.get("update", [])
            for item in updated:
                if item.get("error"):
                    log.error(f"Błąd podczas aktualizacji zniżki (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Zaktualizowano zniżkę '{item.get('name', 'N/A')}' w WooCommerce (ID: {item.get('id')}).")
            all_updated.extend(updated)
            update_idx += batch_updated_count
            
            # Przetwarzamy usunięte zniżki
            deleted = response.get("delete", [])
            for item in deleted:
                if item.get("error"):
                    log.error(f"Błąd podczas usuwania zniżki (ID: {item.get('id', 'N/A')}): {item.get('error')}")
                    status = False
                else:
                    log.info(f"Usunięto zniżkę z WooCommerce (ID: {item.get('id')}).")
            all_deleted.extend(deleted)
            delete_idx += batch_deleted_count
            
            time.sleep(1)  # Krótkie opóźnienie między partiami aby uniknąć limitów API
            
        except HTTPError as http_err:
            log.error(f"Błąd HTTP podczas batchowej synchronizacji zniżek: {http_err}")
            status = False
            break
        except Exception as e:
            log.error(f"Błąd podczas batchowej synchronizacji zniżek: {e}")
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
    log.debug("Zakończono synchornizacje: " + ", ".join(stats) + " zniżek.")

    return status, all_created, all_updated, all_deleted