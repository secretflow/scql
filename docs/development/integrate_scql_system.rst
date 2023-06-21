How To Integrate SCQL System
============================

Overview
--------

As shown in FIG, SCQL System needs to work with **Client** and **GRM** :

* Client: user interface designed for query submission and result retrieval.

* GRM: help to manage schema information and identify parties.

.. note::
  please read :doc:`../reference/grm` to know more about GRM.

.. image:: ../imgs/scql_system.jpeg

Therefore the platform should support Client and GRM to integrate SCQL System.

Specifically, the Client interact with SCDB through Query API,  while GRM interact with SCDB through GRM API.

Query API
----------
For SQL query, SCDB support services:

* Submit: async API, just submit SQL query and return, server listen on ``${SCDBHost}/public/submit_query``
* Fetch: async API, try to get the result of a SQL query, server listen on ``${SCDBHost}/public/fetch_result``
* SubmitAndGet: sync API, submit query and wait to get the query result, server listen on ``${SCDBHost}/public/submit_and_get``

Please refer to :doc:`../development/scql_api` for details.

.. note::
  *  Client can choose to support either async or sync API according to business scenarios:

      If the SQL query task might take too much time, it is recommended to use the async API, otherwise use the sync API for simplicity.


In a word, the custom Client should construct HTTP request for user's SQL, post to SCDB and parse the response from SCDB.

GRM API
--------
SCQL uses rpc API to communicate with GRM:

* ``/GetTableMeta``: fetch metadata of table, GRM server should listen on ``${GRMServerHost}/GetTableMeta``
* ``/GetEngines``: get endpoints for specific SCQLEngines(parties), GRM server should listen on ``${GRMServerHost}/GetEngines``
* ``/VerifyTableOwnership``: check whether user has ownership of table, GRM server should listen on ``${GRMServerHost}/VerifyTableOwnership``

Please refer to :doc:`../reference/grm` for details.

The custom GRM Server needs to support above services corresponding to the GRM API.
