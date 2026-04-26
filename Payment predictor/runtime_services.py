import copy
import logging
import threading
import time

logger = logging.getLogger(__name__)


class ForecastSnapshotCache:
    def __init__(self, ttl_seconds=300):
        self.ttl_seconds = max(int(ttl_seconds or 0), 0)
        self.lock = threading.Lock()
        self._items = {}

    def _purge_locked(self):
        if not self._items:
            return
        now = time.time()
        expired_keys = [
            key for key, item in self._items.items()
            if now - item["stored_at"] > self.ttl_seconds
        ]
        for key in expired_keys:
            self._items.pop(key, None)

    def get(self, key):
        if self.ttl_seconds <= 0:
            return None
        with self.lock:
            self._purge_locked()
            item = self._items.get(key)
            if not item:
                return None
            return copy.deepcopy(item["value"])

    def set(self, key, value):
        if self.ttl_seconds <= 0:
            return copy.deepcopy(value)
        with self.lock:
            self._purge_locked()
            self._items[key] = {
                "stored_at": time.time(),
                "value": copy.deepcopy(value),
            }
        return copy.deepcopy(value)

    def clear(self):
        with self.lock:
            self._items.clear()


class BackgroundRefreshCoordinator:
    def __init__(self, knowledge_base, cash_out_store, forecast_cache, interval_seconds):
        self.knowledge_base = knowledge_base
        self.cash_out_store = cash_out_store
        self.forecast_cache = forecast_cache
        self.interval_seconds = max(int(interval_seconds or 0), 0)
        self.thread = None
        self.stop_event = threading.Event()

    def refresh_all(self):
        knowledge_ok = self.knowledge_base.refresh_data()
        cash_out_ok = self.cash_out_store.refresh_data() if self.cash_out_store else False
        self.forecast_cache.clear()
        return {
            "knowledgeBase": knowledge_ok,
            "cashOutSource": (
                cash_out_ok
                if self.cash_out_store and self.cash_out_store.client.is_configured()
                else None
            ),
        }

    def start(self):
        if self.interval_seconds <= 0 or self.thread is not None:
            return

        def _runner():
            while not self.stop_event.wait(self.interval_seconds):
                try:
                    self.refresh_all()
                except Exception:
                    logger.exception("Periodic data refresh failed.")

        self.thread = threading.Thread(
            target=_runner,
            name="background-data-refresh",
            daemon=True,
        )
        self.thread.start()
