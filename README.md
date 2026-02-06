# ERPFlow

Narzędzie do synchronizacji między Comarch ERP Optima i WooCommerce.


## Użycie

```bash
python src/main.py [opcje]
```

### Opcje

- `--obejmuj-darmowe-towary` - Synchronizuj również darmowe towary (cena = 0). Domyślnie wyłączone.
- `--setup` - Inicjalizuje śledzenie zmian w bazie danych. Należy uruchomić przed pierwszą synchronizacją.
- `--odtworz` - Tworzy wszystkie elementy bez względu na istniejące dane.
- `--regeneruj` - Usuwa wszystkie elementy i tworzy je ponownie. Używaj ostrożnie, ponieważ może prowadzić do utraty danych.

# Instalacja

**Instaluj poetry**
```bash
pipx install poetry
```

**Zainstaluj zależności**
```bash
poetry install
```

**Uwaga: upewnij się że masz zainstalowane `unixodbc` na systemie.**