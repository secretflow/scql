Security overview
=================

Security Guarantees and Threat Model
------------------------------------

For a single query, SCQL protects the confidentiality of data that meets the CCL permission requirements during the computation process.

SCQL does not protect queries as queries are designed to be public to all participants in SCQL. SCQL also does not protect the size (dimension) information of intermediate computation results.

SCQL is built on top of the MPC framework `secretflow/spu`_, using a semi-honest security model. The SCQL semi-honest model assumes that all participants, including the query issuer, the data owner (SCQLEngine is deployed on the data owner) and SCDB server, strictly abide by the protocol, but may try to learn others' private data from its legitimately received messages.

.. warning::
    If you select the SEMI2K as SCQL's underlying mpc protocol, please make sure to use the `TrustedThirdParty beaver provider`_ [#f1]_. The other beaver provider mode `TrustedFirstParty beaver provider`_ should only be used for debugging and may incur significant security problem in the production environment.

Like all cryptography-based privacy-preserving computing systems, SCQL at this stage cannot solve the problem of deducing original privacy data based on the results of legal queries. The current academic solution to this problem is generally to add noise into data through differential privacy mechanism. Although the CCL mechanism allows the data owners to restrict the use of their data, which can alleviate risks to a certain extent, it cannot completely eliminate the risks. SCQL also does not solve the problem of participants tampering with their original input to obtain other participants' private information.

The following chapters will describe possible attack methods for inferring data from results, and give corresponding suggestions.


Suggestions on Deployment
-------------------------

The SCDB Server serves as a central coordination component in the SCQL system, responsible for translating the query into a hybrid MPC-plaintext execution graph, and then dispatching it to the SCQLEngines deployed on each individual participant for execution. The SCDB server is responsible for generating execution graphs, and it should not be evil. To dispel participants' concerns about the SCDB server potentially engaging in malicious behavior, it is recommended to deploy the SCDB server in a trusted third party.

If the trusted third party is not exist, you are recommended to deploy SCQL system in P2P mode. refer to :doc:`/topics/system/intro-p2p` for details.


Risk Statement and Suggestion for SCQL Result Inversion Attack
--------------------------------------------------------------

The query of SCQL could be flexible, adversaries may construct adaptive attacks using multiple legit queries or one complex query to achieve the purpose of deriving the original data.


Attack Method 1: Multi-query Attack
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The multi-query attack method includes two attack ways:
(1) One way to obtain the other party's information is to tamper with the input content for each query, while keeping the query itself unchanged. For example, the attacker can obtain all the information of the other party's join key through multiple join queries and tampering with the content of his join key each time.
(2) Another way is to infer the other party's private data by rewriting the query each time and comparing the results of multiple queries. For example, the attacker can use the where condition to limit the input of the aggregation function. The first time the query obtains the aggregation result of N pieces of data, the second time by changing the where condition, the aggregation result of N-1 pieces of data can be obtained, and then the attacker can obtain the original information of 1 piece of data by comparing results.


Attack Method 2: Constructing Complex Query Attack
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This attack method is similar to the multi-query attack method, and its core idea is to write multiple queries into one complex query.
For example, the attacker can perform multiple comparisons on a certain column in one query, and narrow down the range of data to infer the original data.


Suggestions
^^^^^^^^^^^

1. Each data owner is advised to give careful consideration when setting CCL for their own data.
2. When the upstream platform integrates the SCQL system, it's recommended to add an approval process before running the query. The query is submitted to SCQL for execution only after it has been reviewed and confirmed by all data owners.
3. It is recommended to add an audit mechanism, analyze historical queries, and track down information leakage issues.


System Security Configuration Instructions
------------------------------------------

1. SCQL supports HTTPS protocol, it is recommended to enable HTTPS by default. Please see :ref:`SCDB TLS Configuration <scdb-tls>` and :ref:`SCQLEngine TLS Configuration <scqlengine-tls>` for details on how to enable HTTPS for SCDBServer and SCQLEngine.


Suggestions for upstream integrators
------------------------------------

1. It is recommended to add an approval process before submitting any queries to SCQL for execution.
2. It is recommended to add an audit mechanism, analyze historical queries, and track down information leakage issues.
3. It is recommended to divide the use of SCQL into two stages: development stage and production stage, and to adopt different security control measures.

   * The development stage refers to the stage where the query is under development iteration. The data samples used in the development stage must be small-scale data sets that have been desensitized, de-identified, anonymized, and added with noise, aiming to quickly build the data analysis processing flow.
   * The production stage refers to the joint analysis of the query by multiple participating parties to ensure that the task is risk-free or within the acceptance range of multiple participating parties, and is released for production operation. If the related query needs to be changed, it needs to go through multi-party audit and evaluation again. The production stage uses real data, and parties participating in the joint analysis need to: (1) conduct task evaluation and approval before the event; (2) ensure task consistency during the event, and suspend the task in a timely manner if there is any risk; (3) conduct audit after the event, and ensure that potential data leakage risks can be discovered and avoided in case of malicious behavior.



.. rubric:: Footnotes

.. [#f1] SPU SEMI2K protocol adopts a trusted third party for generating Beaver triples for efficiency. In the future, we will consider adding a Beaver provider implementation that does not rely on third parties.


.. _secretflow/spu: https://github.com/secretflow/spu
.. _TrustedThirdParty beaver provider: https://github.com/secretflow/spu/blob/270f6e90c2464a8dba7c681fddf37dcd37adfe32/libspu/spu.proto#L281
.. _TrustedFirstParty beaver provider: https://github.com/secretflow/spu/blob/270f6e90c2464a8dba7c681fddf37dcd37adfe32/libspu/spu.proto#L279

