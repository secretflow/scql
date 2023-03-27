SCQL CCL
========

:ref:`English Version </reference/scql_ccl_intro.rst>`

由于 SCQL 语法灵活多变，查询者可以通过精巧地构造 Query 以从结果中尽可能地推测原始数据敏感信息。因此，在执行查询者提交 Query 前，一般需要获得数据 owner 的审核授权。

人工审核 Query 的方式可以让数据 owner 对数据如何被使用以及结果中是否包含敏感信息泄露等问题进行人工确认。但人工审核方式有如下两方面的问题：1、对审核者有较高的要求，审核者需要具备 Query 分析能力，能够分析出潜在的信息泄露问题；2、执行前审批等待时间长，Query 执行前需要获得 Query 涉及的所有数据 owner 的授权。

SCQL 提供了 Column Control List(CCL) 机制，让数据 owner 可以定义数据的使用方式和披露限制，未通过 CCL 检查的 Query 不允许执行。对于通过 CCL 检查的 Query，审核人员在 CCL 的辅助下专注审核 Query 结果是否可以反推原始敏感数据的问题，降低了审核负担，提高审核效率。需要注意的是，CCL 可以一定程度上缓解数据被滥用的问题，但无法杜绝从查询结果反推原始敏感数据的风险，建议结合事前审批一起，降低结果反推风险。

What is CCL?
^^^^^^^^^^^^

CCL(Column Control List) 限定了列的披露策略，以及允许在列上执行的操作。SCQL承诺遵守这些限定，拒绝执行不满足CCL要求的 query，并保证执行过程不会违背 CCL 规定的数据披露策略。CCL 可以表示为一个三元组 <src_column, dest_party, constraint>，其含义为 src_column 对于 dest_party 的约束策略。

.. note::
   一列数据对于一个参与方当前只能设置一种 CCL constraint。

Types of CCL Constraints
^^^^^^^^^^^^^^^^^^^^^^^^

根据当前已经实现的语法，目前 CCL constraints 一共分为如下 7 个类型。其中前 6 个 constraints 说明数据在何种情况下可以披露出去。

* ``PLAINTEXT``: 允许以任何形式（包括明文）进行计算和披露，没有任何使用上的限制，通常用于非敏感数据，请谨慎使用。
* ``PLAINTEXT_AFTER_JOIN``: 允许作为 INNER JOIN 的 key，经过 JOIN 后可以明文披露。
* ``PLAINTEXT_AFTER_GROUP_BY``: 被约束的列经过 GROUP BY 分组后可以明文披露。
* ``PLAINTEXT_AFTER_AGGREGATE``: 被约束的列经过 Aggregation 操作（如：SUM、AVG、MIN、MAX、COUNT）后的结果可以明文披露。CCL 还限制了 GROUP BY 分组的结果行数，记录行数小于等于3的分组，会被过滤掉，不会出现在最终结果中。
* ``PLAINTEXT_AFTER_COMPARE``: 被约束的列经过 Compare 操作（如：<, >, >=, =, != 等等）后的结果可以明文披露。
* ``ENCRYPTED_ONLY``: 始终以密态的形式参与计算，用于标记非常敏感数据，除了 COUNT 外（SCQL 不保护计算中间结果的size），不允许任何形式的披露。
* ``UNKNOWN``: 未定义，目前默认拒绝以任何形式披露。

.. note::
   请用户根据自己的场景需求和隐私保护需要设置 CCL。攻击者可能会构造满足 CCL 约束的 Query，通过复杂查询或者多次查询方式，从结果推测原始数据敏感信息，相关风险描述和建议见 :ref:`SCQL 安全声明 </reference/scql_security_statement_cn.rst>`。

Advantages of CCL
^^^^^^^^^^^^^^^^^

CCL 的首要作用是，给数据 OWNER 提供约束自己数据如何被使用的能力。CCL 带来的另外一个好处是，可以为 SCQL 执行优化提供 hints。

比如当某一列的 CCL constraint 为 ``PLAINTEXT_AFTER_AGGREGATE`` 时，该列数据经过 SUM 聚合计算后，就可以以明文的状态参与接下里的计算，加快整体的执行效率。

How CCL Works?
^^^^^^^^^^^^^^

Translator 把 query 翻译得到的 Logical Plan 和 CCL 作为输入，经过 CCL 推导构建出带 CCL 的 Logical Plan。Translator 将检查根节点即结果的CCL，仅当结果对 query 发起者的约束为 ``PLAINTEXT`` 时，才允许 query 继续执行，否则拒绝此 query 的执行。

.. image:: ../imgs/logical_plan_with_ccl.png
    :alt: Logical Plan with CCL

下面将对 CCL 的推导过程及原理进行简单介绍和说明。

推导以用户设置的 CCL 为基础，推导方法分为两类，一类是特定操作的推导，比如关系代数操作、特定的函数；另一类是通用表达式推导。

特定操作推导
""""""""""""

下面以 JOIN, GROUP BY 等操作为例，讲解特定操作的推导。

**Join**

当 JOIN keys 的 CCL constraint 均为 ``PLAINTEXT_AFTER_JOIN`` 或 ``PLAINTEXT`` 时，则 JOIN keys 的交集的 CCL constraint 类型为 ``PLAINTEXT``。

**Group By**

当 GROUP BY key 的 CCL constraint 均为 ``PLAINTEXT_AFTER_GROUP_BY`` 或 ``PLAINTEXT`` 时，则 GROUP BY key 经过 GROUP BY 操作后的结果的 CCL constraint 为 ``PLAINTEXT``。

**Aggregate**

当 aggregation functions（如：SUM、AVG、MIN、MAX、COUNT）的入参 CCL constraint 为 ``PLAINTEXT_AFTER_AGGREGATE`` 或 ``PLAINTEXT`` 时，则 aggregation function 的 results 的 CCL constraint 为 ``PLAINTEXT``。

**Compare**

当 compare function（> < = >= <= !=）的入参 CCL constraint 均为 ``PLAINTEXT_AFTER_COMPARE`` 或 ``PLAINTEXT`` 时，则 compare function 的 results 的 CCL constraint 为 ``PLAINTEXT``。

通用表达式推导
""""""""""""""

对于通用 operator/function，根据入参的 CCL constraint 推导其结果的 CCL constraint，可以分为以下几种情况（ ``UNKNOWN`` 属于异常情况，不在此处讨论）：

* 当其中一个入参的 CCL constraint 为 ``PLAINTEXT`` 时，在 CCL constraint 推导时可以该参数不影响结果的 CCL constraint。
* 当其中一个入参的 CCL constraint 为 ``ENCRYPTED_ONLY`` 时，则结果的 CCL constraint 为 ``ENCRYPTED_ONLY``。
* 当入参的 CCL constraint 相同时，返回这个 constraint。
* 对于无入参的 operators，比如 CURDATE(), NOW() 等函数，结果的 CCL constraint 为 ``PLAINTEXT``。

对于 query 中的常量可以认为对所有参与方的 CCL constraint 为 ``PLAINTEXT``。

对于不在上述情况的，目前认为无法进行推导，返回的结果的 CCL constraint 为 ``UNKNOWN``。

CCL 使用示例
""""""""""""

假设当前有两个参与方 Alice 和 Bob，其中 Alice 持有数据 table ta，Bob 持有数据 table tb，两者共同执行一次 sql 查询任务。其中 Alice 持有的 table ta 包含字段 id 和 rank，Bob 持有的 table tb 包含字段 id 和 rank。

Alice 设置 CCL 如下：

* ``<ta.id, Alice, PLAINTEXT>``
* ``<ta.id, Bob, PLAINTEXT_AFTER_JOIN>``
* ``<ta.rank, Alice, PLAINTEXT>``
* ``<ta.rank, Bob, PLAINTEXT_AFTER_COMPARE>``

Bob 设置 CCL 如下：

* ``<tb.id, Bob, PLAINTEXT>``
* ``<tb.id, Alice, PLAINTEXT_AFTER_JOIN>``
* ``<tb.rank, Bob, PLAINTEXT>``
* ``<tb.rank, Alice, PLAINTEXT_AFTER_COMPARE>``

当 Alice 执行 query ``select tb.rank from ta join tb on ta.id = tb.id``，SCQL 会校验 CCL 失败产生并返回错误：tb.rank 对于 Alice 的 CCL constraint 不是 ``PLAINTEXT``。

而当 Alice 执行 query ``select ta.rank > tb.rank from ta join tb on ta.id = tb.id``，这时 CCL 推导得到 ta.rank > tb.rank 对于 Alice 的 CCL constraint 为 ``PLAINTEXT``，校验 CCL 通过，则 query 可以正常执行。