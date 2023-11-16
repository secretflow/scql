SCQL P2P Overview
=================

SCQL supports deployment in P2P mode. In this mode, all parties have the same privileges and do not need to rely on a trusted third party.


Architecture
------------

The SCQL system running in P2P mode consists of ``SCQLBrokers`` and ``SCQLEngines``, each party has its own independent SCQLBroker and SCQLEngines.

- ``SCQLBroker``: As the core module of p2p, it mainly consists of three functions: 
  
  + **Interaction with users**: accept requests and return results through the http interface

  + **P2P status synchronization**: complete status synchronization between different SCQLBrokers.

  + **SQL analysis and job scheduling**: translate SQL query into a hybrid MPC-plaintext execution graph and delegate the execution of the graph to its local SCQLEngine.

- ``SCQLEngine``: SCQLEngine is a hybrid MPC-plaintext execution engine, which collaborates with peers to run the execution graph and reports the query result to SCQLBroker. SCQLEngine is implemented on top of state-of-the-art MPC framework `secretflow/spu`_.

.. image:: /imgs/scql_p2p_architecture.png
    :alt: SCQL P2P Architecture


Workflow of P2P Model
---------------------

In the P2P mode, different parties are identified by ``Unique Identifiers``, and various businesses are isolated by ``Projects``.

- ``Unique Identifier``: including unique partyCode and public-private key pair, the private key should be kept by the party itself secretly, and the public key could be public.
- ``Project``: similar to the virtual database concept in Centralized mode, it is used to isolate different businesses.

The overall workflow can be divided into: **creating project**, **inviting parties**, **creating tables**, **configuring CCLs**, and **executing query jobs**.

1. **Creating Project**: A party can create a project and become the project owner. The owner only has the permission to invite other members to join and has no other additional permissions.
2. **Inviting parties**: After the project owner invites other parties, the other parties can choose whether to accept the invitation. If accepted, they will become project members.
3. **Creating tables**: Project members can create their own tables metadata for joint analysis in the project.
4. **Configuring CCLs**: After members created their own table, they can grant the specific CCL to themselves and other participants.
5. **Executing query jobs**: After completing the CCL configuration, parties can perform the corresponding SQL analysis job.

It is recommended to experience the overall process through the :doc:`/intro/p2p-tutorial`


P2P vs Centralized
------------------

The two modes support the same SQL syntax and MPC protocol, and the end-to-end performance is basically the same.

If there is a trusted party deploying SCDB in the business scenario, it is recommended to use the centralized mode, which is simpler in configuration and integration.
The P2P mode does not rely on trusted parties, but it requires more complex interactions to complete status interactions.



.. _secretflow/spu: https://github.com/secretflow/spu