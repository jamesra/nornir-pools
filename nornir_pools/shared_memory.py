import atexit
import multiprocessing
import os
from multiprocessing.managers import SharedMemoryManager

_shared_memory_manager: SharedMemoryManager | None = None


def _stop_shared_memory_manager():
    global _shared_memory_manager
    if _shared_memory_manager is not None:
        # if 'shutdown' in _shared_memory_manager:
        _shared_memory_manager.shutdown()
        _shared_memory_manager = None


def get_or_create_shared_memory_manager(authkey: bytes | None = None):
    '''
    Obtain a shared memory manager that can be shared between processes. This 
    method should be called from the parent process before it is passed   
    '''
    global _shared_memory_manager

    if _shared_memory_manager is None:
        if 'SHARED_MEMORY_SERVER_ADDRESS' in os.environ:
            authkey = bytes.fromhex(os.environ['SHARED_MEMORY_AUTHKEY'])
            address = os.environ['SHARED_MEMORY_SERVER_ADDRESS']
            _shared_memory_manager = SharedMemoryManager(address=address, authkey=authkey)
            _shared_memory_manager.connect()
        else:
            _authkey = authkey if authkey is not None else multiprocessing.current_process().authkey

            if _shared_memory_manager is None:
                _shared_memory_manager = SharedMemoryManager(authkey=_authkey)
                _shared_memory_manager.start()
                _addr = _shared_memory_manager.address
                assert _addr is not None and _authkey is not None
                print(f"Creating shared memory manager: {_addr}")
                atexit.register(_stop_shared_memory_manager)
                os.environ['SHARED_MEMORY_SERVER_ADDRESS'] = str(_addr)
                os.environ['SHARED_MEMORY_AUTHKEY'] = _authkey.hex()

    return _shared_memory_manager
