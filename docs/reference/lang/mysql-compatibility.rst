MySQL Compatibility
===================

SCQL is highly compatible with MySQL, but there are still some syntax differences.

Unsupported Features
--------------------

1. Partition table
2. Character sets
3. User-defined functions
4. TCL(Transaction Control Language)
5. DML(Data Manipulation Language)

Features that are different from MySQL
--------------------------------------

DDL(Data Definition Language)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CREATE Table
`````````````
The Table created in SCQL is a virtual Table and needs to be mapped to the actual Table, so we extend the statement to transfer the mapping information. For more information, please read :doc:`/reference/lang/manual`.


DQL(Data Query Language)
~~~~~~~~~~~~~~~~~~~~~~~~

About implementation status, please read :doc:`/reference/implementation-status`.

.. note::
    Type Null is unsupported in SCQL.

Functions different from MySQL
``````````````````````````````

+-------------------------------------+---------------------------------------------------------------------------+------------------------+------------+
| Function Name                       | In SCQL                                                                   | In MySQL               | Note       |
+=====================================+===========================================================================+========================+============+
| SUM(INT)                            | returns int                                                               | returns Decimal/Double |            |
+-------------------------------------+---------------------------------------------------------------------------+------------------------+------------+
| Aggregation Functions With Group BY | return groups which have greater or equal ``group_by_threshold`` elements | return all groups      | for safety |
+-------------------------------------+---------------------------------------------------------------------------+------------------------+------------+

.. note::
    ``group_by_threshold`` can be configured in :ref:`p2p mode <config_broker_server_options>` or :ref:`central mode <scdb_config_options>`.

DCL(Data Control Language)
~~~~~~~~~~~~~~~~~~~~~~~~~~

GRANT/REVOKE
````````````

Except permissions same with mysql such as read/write/create/drop..., ccl settings are also required before executing a query. You can change CCL settings via GRANT/REVOKE. About GRANT/REVOKE in SCQL, please read :doc:`/reference/lang/manual`.

Type Conversion Rule
~~~~~~~~~~~~~~~~~~~~

Type conversion takes place when using an operator with operands of different types, to make them compatible. Some conversions occur implicitly.
For example, SCQL automatically converts int to float as necessary

.. code-block:: bash

    user> SELECT alice.plain_long AS alice_long, bob.plain_float AS bob_float, alice.plain_long + bob.plain_float AS sum_result FROM alice INNER JOIN bob ON alice.id = bob.id;
    +------------+-----------+------------+
    | alice_long | bob_float | sum_result |
    +------------+-----------+------------+
    | -1         | -1.5      | -2.5       |
    | 0          | 0.5       | 0.5        |
    | 1          | 1.5       | 2.5        |
    +------------+-----------+------------+

The following rules describe how conversion occurs in SCQL.



single-party query
``````````````````
If a query only involves one participant, SCQL will convert the query into a syntax that conforms to the participant's database (such as MySQL),
and then dispatch it directly to the corresponding database for execution. Thus, for a single-party query, its type conversion rules are generally
consistent with the database used by the participant.

multi-party query
`````````````````
If a query involves multiple participants, SCQL will execute type conversion by applying the following rules.

Compare(>, <, <=, >=, <>, =, !=)
""""""""""""""""""""""""""""""""
* Both arguments in a comparison operation shouldn't be string.
* If both arguments are long, they are compared as long.
* If one of the arguments is float or double, the other argument will be compared as double.

Arithmetic(+, -, \*, /, %)
""""""""""""""""""""""""""
* Arguments of '%' operation only support type long, while other arithmetic operations support types other than type string.
* If both arguments are long, they are calculated as long.
* If one of the arguments is float or double, the other argument will be calculated as double.

Aggregation(sum, count, avg, min, max)
""""""""""""""""""""""""""""""""""""""
* In all aggregation functions except count, parameters should not be type string.
* If all arguments are long, they are calculated as long.
* If one of the arguments is float or double, the other argument will be calculated as double.
