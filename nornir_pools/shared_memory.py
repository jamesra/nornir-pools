import multiprocessing
import os
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.managers import SharedMemoryManager
import atexit

_shared_memory_manager = None #  type: SharedMemoryManager
 
def _stop_shared_memory_manager():
    global _shared_memory_manager
    if _shared_memory_manager is not None:
        #if 'shutdown' in _shared_memory_manager:
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
            _shared_memory_authkey = multiprocessing.current_process().authkey if authkey is None else authkey

            if _shared_memory_manager is None:
                _shared_memory_manager = SharedMemoryManager(authkey=authkey)
                _shared_memory_manager.start()
                print(f"Creating shared memory manager: {_shared_memory_manager.address}")
                atexit.register(_stop_shared_memory_manager)
                os.environ['SHARED_MEMORY_SERVER_ADDRESS'] = _shared_memory_manager.address
                os.environ['SHARED_MEMORY_AUTHKEY'] = authkey.hex()

    return _shared_memory_manager

