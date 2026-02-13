import connections as con
import logger as log

def get_prices(business_id=None, product_id=None):
    """
    Pobiera listę cen z ERPFlow.
    
    Args:
        business_id (int, optional): ID firmy do filtrowania.
        product_id (int, optional): ID produktu do filtrowania.
        
    Returns:
        list: Lista słowników z danymi o cenach. Zwraca pustą listę w przypadku błędu.
    """
    params = {}
    if business_id:
        params['business_id'] = business_id
    if product_id:
        params['product_id'] = product_id
        
    try:
        response = con.efapi.get("prices", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd pobierania cen: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log.error(f"Wyjątek podczas pobierania cen: {e}")
        return []

def create_price(data):
    """
    Tworzy nowy wpis ceny w ERPFlow.
    
    Args:
        data (dict): Słownik z danymi ceny (np. business_id, product_id, price).
        
    Returns:
        dict: Utworzony obiekt ceny lub słownik z błędem.
    """
    try:
        response = con.efapi.post("prices", data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Błąd tworzenia ceny: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas tworzenia ceny: {e}")
        return {"error": str(e)}

def update_price_by_product(data):
    """
    Aktualizuje cenę na podstawie business_id i product_id.
    
    Args:
        data (dict): Dane do aktualizacji. Musi zawierać business_id i product_id.
        
    Returns:
        dict: Zaktualizowany obiekt ceny lub słownik z błędem.
    """
    try:
        # Endpoint POST /prices (tak jak przy tworzeniu, ale logika serwera obsługuje aktualizację jeśli istnieje)
        response = con.efapi.post("prices", data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Błąd aktualizacji ceny (business/product): {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas aktualizacji ceny (business/product): {e}")
        return {"error": str(e)}

def get_price_by_id(price_id):
    """
    Pobiera dane ceny na podstawie ID.
    
    Args:
        price_id (int): ID wpisu ceny.
        
    Returns:
        dict: Obiekt ceny lub None w przypadku błędu.
    """
    try:
        response = con.efapi.get(f"prices/{price_id}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd pobierania ceny {price_id}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        log.error(f"Wyjątek podczas pobierania ceny {price_id}: {e}")
        return None

def update_price_by_id(price_id, data):
    """
    Aktualizuje cenę na podstawie ID wpisu.
    
    Args:
        price_id (int): ID wpisu ceny.
        data (dict): Dane do zaktualizowania.
        
    Returns:
        dict: Zaktualizowany obiekt ceny lub słownik z błędem.
    """
    try:
        response = con.efapi.post(f"prices/{price_id}", data)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd aktualizacji ceny {price_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas aktualizacji ceny {price_id}: {e}")
        return {"error": str(e)}

def delete_price(price_id):
    """
    Usuwa wpis ceny na podstawie ID.
    
    Args:
        price_id (int): ID wpisu ceny.
        
    Returns:
        dict: Odpowiedź API lub słownik z błędem.
    """
    try:
        response = con.efapi.delete(f"prices/{price_id}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd usuwania ceny {price_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas usuwania ceny {price_id}: {e}")
        return {"error": str(e)}

def batch_prices(create=None, update=None, delete=None, upsert=None):
    """
    Wykonuje operacje batchowe na cenach.
    
    Args:
        create (list, optional): Lista obiektów cen do utworzenia.
        update (list, optional): Lista obiektów cen do aktualizacji.
        delete (list, optional): Lista ID cen do usunięcia.
        upsert (list, optional): Lista obiektów cen do utworzenia lub aktualizacji.
        
    Returns:
        dict: Wynik operacji batchowej.
    """
    data = {}
    if create:
        data['create'] = create
    if update:
        data['update'] = update
    if delete:
        data['delete'] = delete
    if upsert:
        data['upsert'] = upsert
        
    if not data:
        return {}

    try:
        response = con.efapi.post("prices/batch", data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Błąd operacji batchowej cen: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas operacji batchowej cen: {e}")
        return {"error": str(e)}

# Visibility API (API Widoczności)

def get_visibility_rules():
    """
    Pobiera listę reguł widoczności.
    
    Returns:
        list: Lista reguł widoczności.
    """
    try:
        response = con.efapi.get("visibility")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd pobierania reguł widoczności: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        log.error(f"Wyjątek podczas pobierania reguł widoczności: {e}")
        return []

def create_visibility_rule(data):
    """
    Tworzy nową regułę widoczności.
    
    Args:
        data (dict): Dane reguły widoczności.
        
    Returns:
        dict: Utworzona reguła lub słownik z błędem.
    """
    try:
        response = con.efapi.post("visibility", data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Błąd tworzenia reguły widoczności: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas tworzenia reguły widoczności: {e}")
        return {"error": str(e)}

def get_visibility_rule_by_id(rule_id):
    """
    Pobiera regułę widoczności po ID.
    
    Args:
        rule_id (int): ID reguły.
        
    Returns:
        dict: Reguła widoczności lub None.
    """
    try:
        response = con.efapi.get(f"visibility/{rule_id}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd pobierania reguły widoczności {rule_id}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        log.error(f"Wyjątek podczas pobierania reguły widoczności {rule_id}: {e}")
        return None

def update_visibility_rule(rule_id, data):
    """
    Aktualizuje regułę widoczności po ID.
    
    Args:
        rule_id (int): ID reguły.
        data (dict): Dane do aktualizacji.
        
    Returns:
        dict: Zaktualizowana reguła lub słownik z błędem.
    """
    try:
        response = con.efapi.post(f"visibility/{rule_id}", data)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd aktualizacji reguły widoczności {rule_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas aktualizacji reguły widoczności {rule_id}: {e}")
        return {"error": str(e)}

def delete_visibility_rule(rule_id):
    """
    Usuwa regułę widoczności po ID.
    
    Args:
        rule_id (int): ID reguły.
        
    Returns:
        dict: Odpowiedź API lub słownik z błędem.
    """
    try:
        response = con.efapi.delete(f"visibility/{rule_id}")
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd usuwania reguły widoczności {rule_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas usuwania reguły widoczności {rule_id}: {e}")
        return {"error": str(e)}

def batch_visibility_rules(create=None, update=None, delete=None):
    """
    Wykonuje operacje batchowe na regułach widoczności.
    
    Args:
        create (list, optional): Lista reguł do utworzenia.
        update (list, optional): Lista reguł do aktualizacji.
        delete (list, optional): Lista ID reguł do usunięcia.
        
    Returns:
        dict: Wynik operacji batchowej.
    """
    data = {}
    if create:
        data['create'] = create
    if update:
        data['update'] = update
    if delete:
        data['delete'] = delete
        
    if not data:
        return {}

    try:
        response = con.efapi.post("visibility/batch", data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            log.error(f"Błąd operacji batchowej reguł widoczności: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas operacji batchowej reguł widoczności: {e}")
        return {"error": str(e)}
