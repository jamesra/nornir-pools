--------
Examples
--------

This is a work in progress.  See the tests for many more examples.


Calling a function on a thread
==============================
::
 
   def Add(x,y):
       return x + y
   
   import nornir_pools as pools
   thread_pool = pools.GetGlobalThreadPool()
   task = thread_pool.add_task("Add 3 + 5", Add, 3,y=5)
   sum = task.wait_return()
   print str(sum)
   
Calling a function on a cluster
===============================
::
 
   def Add(x,y):
       return x + y
   
   import nornir_pools as pools
   thread_pool = pools.GetGlobalClusterPool()
   task = thread_pool.add_task("Add 3 + 5", Add, 3,y=5)
   sum = task.wait_return()
   print str(sum)
 