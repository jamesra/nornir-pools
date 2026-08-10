import atexit
import ast
import multiprocessing
import os
from multiprocessing.managers import SharedMemoryManager

_shared_memory_manager: SharedMemoryManager | None = None


def _stop_shared_memory_manager() -> None:
    global _shared_memory_manager
    if _shared_memory_manager is not None:
        _shared_memory_manager.shutdown()
        _shared_memory_manager = None


def _parse_shared_memory_address(raw: str) -> str | tuple[str, int]:
    """Parse address stored in SHARED_MEMORY_SERVER_ADDRESS (tuple repr or plain string)."""
    raw = raw.strip()
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return raw
    if isinstance(parsed, (tuple, list)) and len(parsed) == 2:
        return (str(parsed[0]), int(parsed[1]))
    return raw


def get_or_create_shared_memory_manager(authkey: bytes | None = None):
    """Obtain a SharedMemoryManager for inter-process buffers.

    Call from the parent before workers start. The listener address/authkey are
    published via SHARED_MEMORY_SERVER_ADDRESS / SHARED_MEMORY_AUTHKEY so children
    can connect to the same manager.
    """
    global _shared_memory_manager

    if _shared_memory_manager is None:
        if 'SHARED_MEMORY_SERVER_ADDRESS' in os.environ:
            key = authkey
            if key is None:
                key = bytes.fromhex(os.environ['SHARED_MEMORY_AUTHKEY'])
            address = _parse_shared_memory_address(os.environ['SHARED_MEMORY_SERVER_ADDRESS'])
            _shared_memory_manager = SharedMemoryManager(address=address, authkey=key)
            _shared_memory_manager.connect()
        else:
            _authkey = authkey if authkey is not None else multiprocessing.current_process().authkey
            _shared_memory_manager = SharedMemoryManager(authkey=_authkey)
            _shared_memory_manager.start()
            _addr = _shared_memory_manager.address
            assert _addr is not None and _authkey is not None
            print(f"Creating shared memory manager: {_addr}")
            atexit.register(_stop_shared_memory_manager)
            # Store a literal-evaluable form so children can reconnect with a real tuple.
            os.environ['SHARED_MEMORY_SERVER_ADDRESS'] = repr(_addr)
            os.environ['SHARED_MEMORY_AUTHKEY'] = _authkey.hex()

    return _shared_memory_manager
