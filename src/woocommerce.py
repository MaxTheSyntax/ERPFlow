import time
import logger as log
import connections as con
from requests.exceptions import HTTPError

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
            response = con.wcapi.post("products/batch", data).json()
            
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