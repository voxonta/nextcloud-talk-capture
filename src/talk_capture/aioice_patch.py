"""Monkey-patch for aioice Transaction.__retry race condition.

Bug: aioice/stun.py Transaction.__retry() calls
  self.__future.set_exception(TransactionTimeout())
without checking if the future is already done.

If a response arrives at the same moment as the retry timeout,
response_received() sets the future first (it HAS the done() check),
then __retry() tries to set_exception on an already-done future
→ asyncio.InvalidStateError → process crash.

Fix: Add `if not self.__future.done()` guard, same as response_received().

See: https://github.com/aiortc/aioice/blob/main/src/aioice/stun.py
"""

import asyncio
from aioice import stun


def _patched_retry(self):
    if self._Transaction__tries >= self._Transaction__tries_max:
        if not self._Transaction__future.done():
            self._Transaction__future.set_exception(stun.TransactionTimeout())
        return

    self._Transaction__protocol.send_stun(
        self._Transaction__request, self._Transaction__addr
    )

    loop = asyncio.get_event_loop()
    self._Transaction__timeout_handle = loop.call_later(
        self._Transaction__timeout_delay, self._Transaction__retry
    )
    self._Transaction__timeout_delay *= 2
    self._Transaction__tries += 1


stun.Transaction._Transaction__retry = _patched_retry
