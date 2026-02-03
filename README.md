# ERPFlow

Narzędzie do synchronizacji między Comarch ERP Optima i WooCommerce.


## Użycie

```bash
python src/main.py [opcje]
```

### Opcje

- `--obejmuj-darmowe-towary` - Synchronizuj również darmowe towary (cena = 0). Domyślnie wyłączone.

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