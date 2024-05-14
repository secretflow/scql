.. SCQL documentation master file, created by
   sphinx-quickstart on Wed Jul 13 19:32:45 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SCQL Documentation
==================

Secure Collaborative Query Language (SCQL) is a system that translates SQL statements into a hybrid MPC-plaintext execution graph and executes them on a federation of database systems. The MPC framework is powered by `SPU <https://github.com/secretflow/spu>`_.


Getting started
---------------

Follow the :doc:`tutorial </intro/p2p-tutorial>` and try out SCQL on your machine!


SCQL Systems
------------

- **Overview**:
  :doc:`System overview and architecture </topics/system/intro>` |
  :doc:`P2P vs. Centralized </topics/system/deploy-arch>`

- **Security**:
  :doc:`Security overview </topics/security/overview>`

- **Reference**:
  :doc:`SCQL implementation status </reference/implementation-status>`


The SCQL Language
-----------------

- **Reference**:
  :doc:`SCQL language manual </reference/lang/manual>` |
  :doc:`Compatibility with MySQL </reference/lang/mysql-compatibility>`


Column Control List (CCL)
-------------------------

- **Overview**:
  :doc:`Introduction to CCL </topics/ccl/intro>`

- **Guides**:
  :doc:`Common usage and advice </topics/ccl/usage>`

- **Reference**:
  :ref:`GRANT and REVOKE <scql_grant_revoke>` |
  :ref:`How CCL works <how_ccl_works>`


Clients
-------

* **P2P**

  - :doc:`Broker HTTP API reference </reference/broker-api>`


* **Centralized**

  - :doc:`Overview of SCQL clients </topics/clients/integrate-scdb>`
  - :doc:`SCDB HTTP API reference </reference/scdb-api>`


Deployment
----------

* **P2P**

  - **Guides**: :doc:`P2P Deployment </topics/deployment/how-to-deploy-p2p-cluster>`
  - **Reference**: :doc:`P2P Configuration </reference/p2p-deploy-config>`

* **Centralized**

  - **Guides:** :doc:`Centralized Deployment </topics/deployment/how-to-deploy-centralized-cluster>`
  - **Reference**: :doc:`Centralized Configuration </reference/centralized-deploy-config>`


* **With Kuscia**: :doc:`Run SCQL on Kuscia </topics/deployment/run-scql-on-kuscia>`


For contributors
----------------

- **Reference**:
  :doc:`SCQL operators </reference/operators>`


.. toctree::
    :hidden:

    intro/index
    topics/index
    reference/index
