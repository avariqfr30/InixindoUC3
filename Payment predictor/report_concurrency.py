import concurrent.futures
import threading
import time


class SectionPassExecutor:
    def __init__(self, max_parallel=1, cooldown_seconds=120, clock=None):
        self.max_parallel = max(int(max_parallel), 1)
        self.cooldown_seconds = max(int(cooldown_seconds), 0)
        self.clock = time.monotonic if clock is None else clock
        self._state_lock = threading.Lock()
        self._global_slots = threading.BoundedSemaphore(self.max_parallel)
        self._degraded_until = 0.0

    def effective_parallelism(self):
        with self._state_lock:
            if self.clock() < self._degraded_until:
                return 1
        return self.max_parallel

    def _invoke(self, worker, item):
        with self._global_slots:
            return worker(item)

    @staticmethod
    def is_recoverable_error(exc):
        status_code = getattr(exc, "status_code", None)
        if status_code in {429, 502, 503, 504}:
            return True
        error_name = type(exc).__name__.lower()
        return "timeout" in error_name or "connect" in error_name

    def _enter_cooldown(self):
        with self._state_lock:
            self._degraded_until = max(
                self._degraded_until,
                self.clock() + self.cooldown_seconds,
            )

    def run(self, items, worker):
        ordered_items = list(items)
        worker_count = min(self.effective_parallelism(), len(ordered_items))
        if worker_count < 2:
            return [self._invoke(worker, item) for item in ordered_items]

        results = [None] * len(ordered_items)
        failures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [pool.submit(self._invoke, worker, item) for item in ordered_items]
            for index, future in enumerate(futures):
                try:
                    results[index] = future.result()
                except Exception as exc:
                    failures.append((index, exc))

        nonrecoverable = [
            failure for failure in failures if not self.is_recoverable_error(failure[1])
        ]
        if nonrecoverable:
            raise nonrecoverable[0][1]

        if failures:
            self._enter_cooldown()
            for index, _exc in failures:
                results[index] = self._invoke(worker, ordered_items[index])

        return results
