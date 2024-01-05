SCQL Language Manual
====================


.. _scql_data_types:

SCQL Data Types
---------------

SCQL supports frequently-used data types, as illustrated in the following table.

+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
|   Data Type   |            Alias             |          Description                                                                                                                                    |
+===============+==============================+=========================================================================================================================================================+
| ``integer``   | ``int``, ``long``, ``int64`` |                                                                                                                                                         |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``float``     | ``float32``                  |                                                                                                                                                         |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``double``    | ``float64``                  |                                                                                                                                                         |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``string``    | ``str``                      |                                                                                                                                                         |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``datetime``  |                              | Used for values that contain both date and time parts. SCQL retrieves and displays in 'YYYY-MM-DD hh:mm:ss' format                                      |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp`` |                              | Used for values that contain both date and time parts. SCQL retrieves in 'YYYY-MM-DD hh:mm:ss' format and displays in int64 value affected by time zone |
+---------------+------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+


.. _scql_statements:

SCQL Query Syntax
-----------------

It is compatible with most MySQL DQL syntax. For syntax differences between SCQL and MySQL, please read :doc:`/reference/lang/mysql-compatibility`.

.. code-block:: SQL

    SELECT [DISTINCT] select_expr [, select_expr] ...
    [FROM table_reference]
    [WHERE where_condition]
    [GROUP BY column]
    [into_option]

    select_expr:
        col_reference [AS alias]

    col_reference:
        column
    | agg_function(column)

    column:
        *
    | db_name.tbl_name.col_name field_as_name_opt
    | alias.col_name field_as_name_opt
    | expression field_as_name_opt

    field_as_name_opt:
        ""
    | field_as_name

    field_as_name:
        identifier
    | "AS" identifier

    table_reference:
        table_factor
    | join_table
    | union_table

    table_factor:
        db_name.tbl_name [[AS] alias]

    join_table:
        table_reference [INNER] JOIN table_factor [join_specification]

    union_table:
        select_expr
        | UNION [ALL] union_table

    join_specification:
        ON search_condition

    expression:
        expression "SUPPORTED_OP" expression
        | "NOT" expression
        | predicate_expr

    predicate_expr:
        column InOrNotOp '(' expression_list ')'
        | column InOrNotOp sub_select
        | column

    sub_select:
        '(' select_stmt ')'

    into_option:
        INTO OUTFILE PARTY_CODE 'party_code' 'file_path' [export_options]

    export_options:
        FIELDS TERMINATED BY 'terminal_character'



Functions and Operators
-----------------------

.. todo:: this part is not ready, please check later
