import threading


class CachedObject(object):
    def __init__(self, executor, worker):
        self.lock = threading.RLock()
        self._executor = executor
        self._worker = worker
        self._refreshing = False
        self._cached = self._refresh()

    def get(self):
        with self.lock:
            if not self._refreshing:
                self._refresh()

        return self._cached.result()

    def _refresh(self):
        """Updates the current value in the background.

        :rtype: concurrent.futures.Future
        """
        assert not self._refreshing

        def on_finished(result_future):
            with self.lock:
                self._refreshing = False
                self._cached = result_future

        # start updating
        self._refreshing = True
        result = self._executor.submit(self._worker)
        result.add_done_callback(on_finished)
        return result


def cached(executor, function, *args, **kwargs):
    cache = CachedObject(executor, lambda: function(*args, **kwargs))
    return cache.get
