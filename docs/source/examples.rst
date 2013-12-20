--------------
Example of use
--------------

nornir_pools attempts to make the backend of the pool invoking the function as transaparent as possible.  Code to run on a local thread vs a cluster should be the same:

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
