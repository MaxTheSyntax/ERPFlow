import connections as con
import logger as log
import secrets
import string

def generate_random_password(length=12):
    """Generuje losowe hasło."""
    alphabet = string.ascii_letters + string.digits
    password = ''.join(secrets.choice(alphabet) for i in range(length))
    return password

def create_user(data):
    """Tworzy nowego użytkownika WordPress."""
    try:
        if 'password' not in data:
            data['password'] = generate_random_password()
        
        response = con.wpapi.post("users", data)
        if response.status_code == 201:
            return response.json()
        else:
            log.error(f"Błąd tworzenia użytkownika: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas tworzenia użytkownika: {e}")
        return {"error": str(e)}

def update_user(user_id, data):
    """Aktualizuje istniejącego użytkownika WordPress."""
    try:
        response = con.wpapi.post(f"users/{user_id}", data)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd aktualizacji użytkownika {user_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas aktualizacji użytkownika {user_id}: {e}")
        return {"error": str(e)}

def delete_user(user_id, reassign=0):
    """Usuwa użytkownika WordPress."""
    try:
        params = {"force": True, "reassign": reassign}
            
        response = con.wpapi.delete(f"users/{user_id}", params=params)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Błąd usuwania użytkownika {user_id}: {response.status_code} - {response.text}")
            return {"error": response.text}
    except Exception as e:
        log.error(f"Wyjątek podczas usuwania użytkownika {user_id}: {e}")
        return {"error": str(e)}

def batch_sync_users(creations=None, updates=None, deletions=None):
    """
    Symuluje batchową synchronizację użytkowników (WP API nie wspiera natywnego batcha dla users).
    """
    creations = creations or []
    updates = updates or []
    deletions = deletions or []
    
    created_items = []
    updated_items = []
    deleted_items = []
    success = True
    
    # Tworzenie
    for data in creations:
        result = create_user(data)
        if "error" in result:
            success = False
            result["sku"] = data.get("username") # Zachowujemy identyfikator dla logowania błędów
        created_items.append(result)
        
    # Aktualizacja
    for data in updates:
        user_id = data.get("id")
        if not user_id:
            log.error(f"Brak ID dla aktualizacji użytkownika: {data}")
            continue
        result = update_user(user_id, data)
        if "error" in result:
            success = False
        updated_items.append(result)
        
    # Usuwanie
    for user_id in deletions:
        result = delete_user(user_id)
        if "error" in result:
            success = False
        deleted_items.append(result)
        
    return success, created_items, updated_items, deleted_items
