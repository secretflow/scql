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
  :doc:`Common usage </topics/system/usage>` |
  :doc:`P2P </topics/system/intro-p2p>`

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

- :doc:`Overview of SCQL clients </topics/clients/overview>`
- :doc:`HTTP API reference </reference/http-api>`


Deployment
----------

- **Guides**:
  :doc:`How to deploy an SCQL cluster in centralized mode</topics/deployment/how-to-deploy-centralized-cluster>` |
  :doc:`How to deploy an SCQL cluster in P2P mode</topics/deployment/how-to-deploy-p2p-cluster>`

- **Reference**:
  :doc:`SCQL system config </reference/engine-config>`


For contributors
----------------

- **Reference**:
  :doc:`SCQL operators </reference/operators>`


.. toctree::
    :hidden:

    intro/index
    topics/index
    reference/index
