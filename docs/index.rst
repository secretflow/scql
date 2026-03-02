.. SCQL documentation master file, created by
   sphinx-quickstart on Wed Jul 13 19:32:45 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SCQL Documentation
==================

Secure Collaborative Query Language (SCQL) is a system that translates SQL statements into a hybrid MPC-plaintext execution graph and executes them on a federation of database systems. The MPC framework is powered by `SPU <https://github.com/secretflow/spu>`_.

.. important::

   **You are viewing SCQL 2.0 OpenCore documentation.**

   SCQL 2.0 uses a native **Compiler + Engine** architecture. Previous components (SCDB, SCQLBroker, CCL) are no longer supported.

   Looking for SCQL 1.x documentation? Visit `SCQL 1.0.0b1 docs <https://www.secretflow.org.cn/en/docs/scql/1.0.0b1/>`_.


Getting started
---------------

Follow the :doc:`OpenCore Quickstart </intro/opencore-quickstart>` to get started with SCQL's native compiler + engine architecture.


SCQL Systems
------------

- **Overview**:
  :doc:`SCQL system overview </topics/system/intro>`

- **Security**:
  :doc:`Security overview </topics/security/overview>`

- **Reference**:
  :doc:`SCQL implementation status </reference/implementation-status>`


The SCQL Language
-----------------

- **Reference**:
  :doc:`SCQL language manual </reference/lang/manual>` |
  :doc:`Compatibility with MySQL </reference/lang/mysql-compatibility>`


Deployment
----------

.. note::
   The previous deployment modes (P2P, Centralized, and Kuscia) are **deprecated and no longer supported**.

   **Recommended approach**: Use native compiler + engine integration.

   - See ``examples/opencore-demo/`` for integration examples


For contributors
----------------

- **Reference**:
  :doc:`SCQL operators </reference/operators>`


.. toctree::
    :hidden:

    intro/index
    topics/index
    reference/index
