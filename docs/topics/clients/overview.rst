How To Integrate SCQL System
============================

Overview
--------

As shown in FIG, SCQL System needs to work with **Client**:

* Client: user interface designed for query submission and result retrieval.

.. image:: /imgs/scql_system.png

Therefore the platform should support Client to integrate SCQL System.

Specifically, the Client interact with SCDB through Query API.

Query API
----------
For SQL query, SCDB support services:

* Submit: async API, just submit SQL query and return, server listen on ``${SCDBHost}/public/submit_query``
* Fetch: async API, try to get the result of a SQL query, server listen on ``${SCDBHost}/public/fetch_result``
* SubmitAndGet: sync API, submit query and wait to get the query result, server listen on ``${SCDBHost}/public/submit_and_get``

Please refer to :doc:`/reference/http-api` for details.

.. note::
  *  Client can choose to support either async or sync API according to business scenarios:

      If the SQL query task might take too much time, it is recommended to use the async API, otherwise use the sync API for simplicity.


In a word, the custom Client should construct HTTP request for user's SQL, post to SCDB and parse the response from SCDB.

