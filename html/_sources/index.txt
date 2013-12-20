.. Nornir Pools documentation master file, created by
   sphinx-quickstart on Wed Dec 18 15:14:45 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Nornir Pools's documentation!
========================================

Contents:

.. toctree::
   :maxdepth: 2
   
   api
   examples
    
About
-----

nornir_pools attempts to provide a simple consistent interface around four different threading and clustering options available to Python developers:

* threading
* multiprocessing 
* subprocess
* Parallel Python (pp) for use on clusters

Installation
------------

nornir_pools is pure python library available via Github::

  pip install git+https://github.com/nornir/nornir-pools.git --upgrade

Code sample : Calling a function on a thread vs a cluster
---------------------------------------------------------
   
Thread Pool version

::

   def Add(x,y):
       return x + y
   
   import nornir_pools as pools
   thread_pool = pools.GetGlobalThreadPool()
   task = thread_pool.add_task("Add 3 + 5", Add, 3,y=5)
   sum = task.wait_return()
   print str(sum)

Cluster pool version

::   

   def Add(x,y):
       return x + y
   
   import nornir_pools as pools
   cluster_pool = pools.GetGlobalClusterPool()
   task = cluster_pool.add_task("Add 3 + 5", Add, 3,y=5)
   sum = task.wait_return()
   print str(sum)

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

