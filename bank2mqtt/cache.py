import os
import shelve
from pathlib import Path
from typing import Any, Optional, Dict, Iterator
from platformdirs import user_cache_dir

from loguru import logger


class Cache:
    """
    Classe de cache persistant qui stocke des données clé-valeur dans des fichiers.
    Utilise le dossier de configuration standard du système
    (ex: ~/.config/bank2mqtt sous Linux).
    """

    def __init__(
        self,
        *path: str,
        cache_dir: Optional[str] = None,
        app_name: str = "bank2mqtt",
    ):
        """
        Initialise le cache.

        Args:
            cache_dir: Dossier de cache personnalisé. Si None, utilise le
                      dossier de config standard.
            app_name: Nom de l'application pour le dossier de config.
        """
        if cache_dir is None:
            cache_dir = user_cache_dir(app_name)

        self.cache_dir = Path(os.path.join(cache_dir, *path))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "cache.db"

        logger.debug(f"Cache initialized at: {self.cache_file}")
        logger.debug(f"Cache directory: {self.cache_dir}")

    def __enter__(self):
        """Support du context manager."""
        self._shelf = shelve.open(str(self.cache_file))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ferme proprement le cache."""
        if hasattr(self, "_shelf"):
            self._shelf.close()

    def _get_shelf(self):
        """Obtient une instance du shelf, l'ouvre si nécessaire."""
        if not hasattr(self, "_shelf"):
            self._shelf = shelve.open(str(self.cache_file))
        return self._shelf

    def set(self, key: str, value: Any) -> None:
        """
        Stocke une valeur dans le cache.

        Args:
            key: Clé de stockage
            value: Valeur à stocker (doit être sérialisable)
        """
        logger.debug(f"Setting cache key: {key}")
        shelf = self._get_shelf()
        shelf[key] = value
        shelf.sync()  # Force l'écriture sur disque
        logger.trace(f"Cache key '{key}' set successfully")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Récupère une valeur du cache.

        Args:
            key: Clé à récupérer
            default: Valeur par défaut si la clé n'existe pas

        Returns:
            La valeur stockée ou la valeur par défaut
        """
        shelf = self._get_shelf()
        value = shelf.get(key, default)
        status = "found" if key in shelf else "not found"
        logger.debug(f"Getting cache key: {key} -> {status}")
        return value

    def delete(self, key: str) -> bool:
        """
        Supprime une clé du cache.

        Args:
            key: Clé à supprimer

        Returns:
            True si la clé existait et a été supprimée, False sinon
        """
        shelf = self._get_shelf()
        if key in shelf:
            del shelf[key]
            shelf.sync()
            logger.debug(f"Cache key '{key}' deleted successfully")
            return True
        logger.debug(f"Cache key '{key}' not found for deletion")
        return False

    def exists(self, key: str) -> bool:
        """
        Vérifie si une clé existe dans le cache.

        Args:
            key: Clé à vérifier

        Returns:
            True si la clé existe, False sinon
        """
        shelf = self._get_shelf()
        return key in shelf

    def clear(self) -> None:
        """Vide complètement le cache."""
        shelf = self._get_shelf()
        item_count = len(shelf)
        shelf.clear()
        shelf.sync()
        logger.warning(f"Cache cleared - {item_count} items removed")

    def keys(self) -> Iterator[str]:
        """Retourne un itérateur sur toutes les clés du cache."""
        shelf = self._get_shelf()
        return iter(shelf.keys())

    def items(self) -> Iterator[tuple]:
        """Retourne un itérateur sur tous les couples (clé, valeur) du cache."""
        shelf = self._get_shelf()
        return iter(shelf.items())

    def to_dict(self) -> Dict[str, Any]:
        """Convertit le cache en dictionnaire."""
        shelf = self._get_shelf()
        return dict(shelf)

    def __getitem__(self, key: str) -> Any:
        """Support de l'accès par index: cache[key]"""
        value = self.get(key)
        if value is None and not self.exists(key):
            raise KeyError(f"Key '{key}' not found in cache")
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        """Support de l'assignation par index: cache[key] = value"""
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        """Support de la suppression par index: del cache[key]"""
        if not self.delete(key):
            raise KeyError(f"Key '{key}' not found in cache")

    def __contains__(self, key: str) -> bool:
        """Support de l'opérateur 'in': key in cache"""
        return self.exists(key)

    def __len__(self) -> int:
        """Retourne le nombre d'éléments dans le cache."""
        shelf = self._get_shelf()
        return len(shelf)

    def __repr__(self) -> str:
        """Représentation string du cache."""
        return f"Cache(cache_dir='{self.cache_dir}', items={len(self)})"

    def close(self) -> None:
        """Ferme explicitement le cache."""
        if hasattr(self, "_shelf"):
            logger.debug("Closing cache shelf")
            self._shelf.close()
            delattr(self, "_shelf")


# Exemple d'utilisation en tant que singleton pour l'application
_default_cache = None


def get_default_cache() -> Cache:
    """Retourne l'instance de cache par défaut pour l'application."""
    global _default_cache
    if _default_cache is None:
        _default_cache = Cache()
    return _default_cache
