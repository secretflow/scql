SCQL System Overview
====================

Secure Collaborative Query Language (SCQL) is a system that allows multiple distrusting parties to run joint analysis without revealing their private data.

Key Features
------------

* Semi-honest security. SCQL assumes that all parties are semi-honest.
* Support multiple parties (N >= 2).
* Support common SQL select syntax and functions to meet the needs of most scenarios. Please check :doc:`/reference/implementation-status` for details.
* Practical performance.  SCQL has multiple levels of performance optimization.
* Easy to use. SCQL provides relation SQL-like interface.
* Data usage authorization. SCQL provides a mechanism named CCL (Column Control List) for data owner to define their data usage restrictions.

Architecture
------------

The SCQL system supports both centralized and P2P modes. The centralized model relies on a trusted party, but the deployment and configuration process is simpler; P2P does not need to rely on a trusted party. See :doc:`/topics/system/deploy-arch` for details.

The following introduction will be carried out in centralized mode, For P2P mode, please refer to :doc:`/topics/system/intro-p2p`

An SCQL system running in centralized mode consists of an SCDB server and multiple SCQLEngines.

- SCDB server is responsible for translating SQL query into a hybrid MPC-plaintext execution graph and dispatching the execution graph to SCQLEngine of participants.
- SCQLEngine is a hybrid MPC-plaintext execution engine, which collaborates with peers to run the execution graph and reports the query result to SCDB server. SCQLEngine is implemented on top of state-of-the-art MPC framework `secretflow/spu`_.


.. image:: /imgs/scql_architecture.png
    :alt: SCQL Architecture


How SCQL Works
--------------

We will show how SCQL works through the life of the following sample SCQL query Q.

.. code-block:: SQL
    :caption: SCQL query Q

    SELECT AVG(bank_1.deposit), AVG(bank_2.loan)
    FROM bank_1
    INNER JOIN bank_2
    ON bank_1.customer_id = bank_2.customer_id;


Table schema
^^^^^^^^^^^^

Let's have a look at the schema of tables involved in the above query Q.

.. image:: /imgs/the_life_of_scql_query_env.png
    :alt: Table schema

- ``bank_1``
    Party Bank1 owns the table ``bank_1`` in its local database ``DB1``, which has two columns ``customer_id`` and ``deposit``.
- ``bank_2``
    Party Bank2 owns the table ``bank_2`` in its local database ``DB2``, which has two columns ``customer_id`` and ``loan``.


Column Control List (CCL)
^^^^^^^^^^^^^^^^^^^^^^^^^

CCL Form: ``<src_column, dest_party, constraint>``

It means ``src_column`` is accessible to ``dest_party`` with the ``constraint``.

To make the query Q pass the CCL validation, data owner should grant the following CCL.

* Bank1
   * ``<bank_1.customer_id, Bank1, PLAINTEXT>``
   * ``<bank_1.deposit, Bank1, PLAINTEXT>``
   * ``<bank_1.customer_id, Bank2, PLAINTEXT_AFTER_JOIN>``
   * ``<bank_1.deposit, Bank2, PLAINTEXT_AFTER_AGGREGATE>``

* Bank2
   * ``<bank_2.customer_id, Bank2, PLAINTEXT>``
   * ``<bank_2.loan, Bank2, PLAINTEXT>``
   * ``<bank_2.customer_id, Bank1, PLAINTEXT_AFTER_JOIN>``
   * ``<bank_2.loan, Bank1, PLAINTEXT_AFTER_AGGREGATE>``

.. note::
   To learn more about CCL, please read the doc :doc:`/topics/ccl/intro`.


Lifetime of SCQL query
^^^^^^^^^^^^^^^^^^^^^^

.. image:: /imgs/scql_workflow.png
    :alt: SCQL Workflow

Step1. Initialize a Session
"""""""""""""""""""""""""""

SCDB creates a new session for the incoming query, and then authenticates the identity of the query issuer. It will reject the request if authentication fails.


Step2. Parse and Plan Q
"""""""""""""""""""""""

Parser will parse Q into an AST(Abstract Syntax Tree), and then Planner converts it into a Logical Plan.

.. image:: /imgs/logicalplan_for_Q.png
    :alt: Logical Plan for Q


Step3. Translate
""""""""""""""""

Step3.1 Build and Check CCL

Translator needs to retrieve CCL from CCL manager, it will build CCL along the logical plan and verify the CCL of root node to ensure Q is legal on data owners' constraints.


Step3.2 Translate

The translator takes the logical plan and CCL as inputs and generates an execution graph for the query Q as follows.

.. image:: /imgs/exe_graph_for_Q.png
    :alt: Execution Graph for Q


Step4. Optimize and Split Graph
"""""""""""""""""""""""""""""""

The graph optimizer will optimize the execution graph, such as node fusion and replicated node elimination.
The optimized execution graph is still a whole graph, graph splitter will split the whole graph into subgraphs based on the parties of the nodes.

.. image:: /imgs/subgraph_for_Q.png
    :alt: subgraphs


Step5. Execute
""""""""""""""

SCDB sends the subgraphs to corresponding SCQLEngine nodes, SCQLEngine cooperates with peers to run the execution graph and reports the final result of Q to SCDB.


.. _secretflow/spu: https://github.com/secretflow/spu
