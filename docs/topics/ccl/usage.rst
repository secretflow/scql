CCL Setting Suggestions
=======================

This document will give some examples on CCL settings in typical scenarios. You can set CCL according to your needs in practical scenarios.

Pre-knowledge
-------------

What is SCQL CCL? Please read :doc:`/topics/ccl/intro`.

Grant CCL syntax, please read :ref:`Grant/Revoke CCL<scql_grant_revoke>`.

Examples
--------

This part will give suggestions for CCL settings based on different query scenario examples.

Let's take the joint analysis between party Alice and Bob as examples to illustrate. Party Alice owns table ``t1`` which has fields ``id``, ``credit_rank``, ``income``, ``join_date`` and ``age``.  Party Bob owns table ``t2`` which has fields ``id``, ``age``, ``order_amount``, ``is_active``, ``admin_date``, ``out_date``.

We represent the CCL settings with a 3-tuple form as follows.

``<src_column, dest_party, constraint>``

It means ``src_column`` is accessible to ``dest_party`` with the ``constraint``.
If multiple columns have the same CCL for a party, it will be abbreviated as follows.

``<[src_column1,src_column2,...], dest_party, constraint>``.

Get Intersection Case
^^^^^^^^^^^^^^^^^^^^^

Get the intersection of ids using join
""""""""""""""""""""""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<id, Alice, PLAINTEXT>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_JOIN>``
  * ``<id, Bob, PLAINTEXT>``

.. code-block:: SQL

    -- Both Alice and Bob can get the intersection of ids
    select t1.id from t1 join t2 on t1.id = t2.id;

Get ids both in t1.id and in t2.id using in
"""""""""""""""""""""""""""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_COMPARE>``
  * ``<id, Alice, PLAINTEXT>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_COMPARE>``
  * ``<id, Bob, PLAINTEXT>``

.. code-block:: SQL

    -- Both Alice and Bob can get the results
    -- Alice get the results by executing this query
    select id from t1 where t1.id in (select id from t2);
    -- Bob get the results by executing this query
    select id from t2 where t2.id in (select id from t1);

**More restricted CCL**

CCL settings of table t1 at Alice
  * ``<id, Bob, ENCRYPTED_ONLY>``
  * ``<id, Alice, PLAINTEXT>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_COMPARE>``
  * ``<id, Bob, PLAINTEXT>``

.. code-block:: SQL

    -- Only Alice can get the results. An error occurs when Bob executes this query
    select id from t1 where t1.id in (select id from t2);

Aggregation Case
^^^^^^^^^^^^^^^^

Analyze data using aggregation functions with group by
""""""""""""""""""""""""""""""""""""""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, income, credit_rank], Alice, PLAINTEXT>``
  * ``<[age, income], Bob, PLAINTEXT_AFTER_AGGREGATE>``
  * ``<credit_rank, Bob, PLAINTEXT_AFTER_GROUP_BY>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, order_amount, is_active], Bob, PLAINTEXT>``
  * ``<[age, order_amount], Alice, PLAINTEXT_AFTER_AGGREGATE>``
  * ``<is_active, Alice, PLAINTEXT_AFTER_GROUP_BY>``

.. code-block:: SQL

    -- Query can be executed by user Alice/Bob and get the results
    select t1.credit_rank, t2.is_active, count(*), max(t1.age), min(t1.age), avg(t1.income), sum(t1.income) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*) <= 5;
    select t1.credit_rank, t2.is_active, count(*), max(t2.age), min(t2.age), avg(t2.order_amount), sum(t2.order_amount) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*) <= 5;

**More restricted CCL**

If Alice don't want to reveal age/income info to Bob, just set group keys' CCL constraint to ``ENCRYPTED_ONLY``. Bob's CCL settings remain the same as before, modify the CCL settings of Alice to the following configuration

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, income, credit_rank], Alice, PLAINTEXT>``
  * ``<[age, income], Bob, ENCRYPTED_ONLY>``
  * ``<credit_rank, Bob, PLAINTEXT_AFTER_GROUP_BY>``

.. code-block:: SQL

    -- Query can be executed by user Alice and get the results
    select t1.credit_rank, t2.is_active, count(*), max(t1.age), min(t1.age), avg(t1.income), sum(t1.income) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*) <= 5;
    -- Query can be executed by user Alice/Bob and get the results
    select t1.credit_rank, t2.is_active, count(*), max(t2.age), min(t2.age), avg(t2.order_amount), sum(t2.order_amount) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*) <= 5;

Analyze data using aggregation functions without group by
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, income], Alice, PLAINTEXT>``
  * ``<[age, income], Bob, PLAINTEXT_AFTER_AGGREGATE>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, order_amount], Bob, PLAINTEXT>``
  * ``<[age, order_amount], Alice, PLAINTEXT_AFTER_AGGREGATE>``

**Queries without group by**

.. code-block:: SQL

    -- Query can be executed by user Alice/Bob and get the results
    select count(*), max(t1.age), min(t1.age), avg(t1.income), sum(t1.income) from t1 join t2 on t1.id = t2.id;
    select count(*), max(t2.age), min(t2.age), avg(t2.order_amount), sum(t2.order_amount) from t1 join t2 on t1.id = t2.id;


Filter data before analyzing it
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Case 1: Filter data
"""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, income, credit_rank], Alice, PLAINTEXT>``
  * ``<age, Bob, PLAINTEXT_AFTER_COMPARE>``
  * ``<income, Bob, PLAINTEXT_AFTER_AGGREGATE>``
  * ``<credit_rank, Bob, PLAINTEXT_AFTER_GROUP_BY>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, order_amount, is_active], Bob, PLAINTEXT>``
  * ``<age, Alice, PLAINTEXT_AFTER_COMPARE>``
  * ``<order_amount, Alice, PLAINTEXT_AFTER_AGGREGATE>``
  * ``<is_active, Alice, PLAINTEXT_AFTER_GROUP_BY>``

.. code-block:: SQL

    -- Query can be executed by user Alice/Bob and get the results
    select count(*), sum(t1.income) from t1 join t2 on t1.id = t2.id where t1.age > t2.age group by t1.credit_rank, t2.is_active having count(*) <= 5;
    select count(*), sum(t2.order_amount) from t1 join t2 on t1.id = t2.id where t1.age > t2.age group by t1.credit_rank, t2.is_active having count(*) <= 5;

Case 2: Get the results of compare operators
""""""""""""""""""""""""""""""""""""""""""""

CCL settings of table t1 at Alice
  * ``<id, Bob, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, income, join_date], Alice, PLAINTEXT>``
  * ``<[join_date, income], Bob, PLAINTEXT_AFTER_COMPARE>``

CCL settings of table t2 at Bob
  * ``<id, Alice, PLAINTEXT_AFTER_JOIN>``
  * ``<[id, age, order_amount, admin_date, out_date], Bob, PLAINTEXT>``
  * ``<[age, admin_date, out_date], Alice, ENCRYPTED_ONLY>``
  * ``<order_amount, Alice, PLAINTEXT_AFTER_COMPARE>``

.. code-block:: SQL

    -- Query can be executed by user Bob and get the results
    select t2.id, t2.age, t2.age in (50, 60, 70) as r, (t1.join_date >= t2.admin_date) and (t1.join_date <= t2.out_date) as in_home from t1 join t2 on t1.id = t2.id where t1.income > t2.order_amount * 100;


Data Insensitive Case/Test Case
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the data is not sensitive or test case, you can set all data to PLAINTEXT, then almost all queries will not be rejected by checking CCL. But you have to be careful, otherwise other users can easily select all your data.

CCL settings of table t1 at Alice
  * ``<[id, age, income, credit_rank], Alice, PLAINTEXT>``
  * ``<[id, age, income, credit_rank], Bob, PLAINTEXT>``

CCL settings of table t2 at Bob
  * ``<[id, age, order_amount, is_active], Alice, PLAINTEXT>``
  * ``<[id, age, order_amount, is_active], Bob, PLAINTEXT>``

.. code-block:: SQL

    -- Query can be executed by user Alice/Bob and get the results
    select t1.id from t1 join t2 on t1.id = t2.id;
    select count(*), max(t1.age), min(t2.age), avg(income), sum(order_amount) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*);
    select max(t1.age), min(t2.age), avg(income), sum(order_amount) from t1 join t2 on t1.id = t2.id group by t1.credit_rank, t2.is_active having count(*) <= 5;
    select t1.age > t2.age, t1.income = t2.order_amount from t1 join t2 on t1.id = t2.id;
    select t1.age, t1.credit_rank from t1 join t2 on t1.id = t2.id where t1.age > t2.age;
    select t2.age, t2.order_amount from t1 join t2 on t1.id = t2.id where t1.age > t2.age;
    select t1.credit_rank + t2.order_amount, t1.credit_rank * t1.income > t2.is_active * t2.order_amount from t1 join t2 on t1.id = t2.id where t1.age + t2.age > 10 and t1.income + t2.order_amount > 3000;

Encrypt Case
^^^^^^^^^^^^

If you think the data is very sensitive, then you can set them as ``ENCRYPTED_ONLY``, then only aggregation function count can be performed.

CCL settings of table t1 at Alice
  * ``<[id, age, income, credit_rank], Alice, PLAINTEXT>``
  * ``<[id, age, income, credit_rank], Bob, ENCRYPTED_ONLY>``

CCL settings of table t2 at Bob
  * ``<[id, age, order_amount, is_active], Alice, ENCRYPTED_ONLY>``
  * ``<[id, age, order_amount, is_active], Bob, PLAINTEXT>``

.. code-block:: SQL

    -- Query can be executed by user Alice/Bob and get the results
    select count(*) from t2;
    select count(*) from t1;
