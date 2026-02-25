Frequently Asked Questions (FAQ)
================================

We will collect some popular questions from users and update this part promptly.

Deploy Issues
-------------

**Q**: System/CPU architectures supported by SCQL

   System: Supports Linux and macOS with M-series chips (but macOS requires self-verification). CPU architectures: ARM and x86-64

**Q**: Network timeout when pulling Go packages/GitHub packages

   Add an appropriate GOPROXY

**Q**: Does SCQL support outsourcing computation

   Not supported for now

Project Preparation Issues
--------------------------

**Q**: What is the data scale supported by SCQL?

   The data scale supported by SCQL is mainly limited by resource configurations (such as network, memory, etc.) and the complexity of the query. With sufficient memory, SCQL can support intersection tasks at the scale of billions. For more detailed scenarios, a benchmark test based on the available resources is needed to determine the exact capacity.

**Q**: How many participating parties does SCQL support?

   SCQL does not have a limit on the number of participating parties in a project, but the number of parties that can simultaneously participate in computations is restricted based on the secure computation protocols used. Specifically, CHEETAH supports only two parties, ABY3 supports only three parties, and SEMI2K supports any number of participating parties.

**Q**: Which syntax does SCQL support?

   SCQL is compatible with MySQL syntax. For specific details, please refer to the documentation. For differences from MySQL syntax, please also refer to :doc:`/reference/lang/manual`.

**Q**: What data sources does the engine support?

   - SCQL directly supports the following data sources:

   1. CSV (including local files, OSS, Minio)
   2. MySQL and databases compatible with the MySQL protocol
   3. Postgres

   - SCQL can be extended to support the following data sources:

   4. On Kuscia, additional support for ODPS. **NOTE:** When using Kuscia, users can register data source information (such as CSV file locations, database connection string of MySQL and Postgres) in Kuscia DomainData. SCQL can then access this information through Kuscia Datamesh and process it accordingly.
   5. SCQL supports the Arrow SQL client, and users can implement their own data sources by providing an Arrow SQL server

Errors Occurred During Execution
--------------------------------

**Q**: The engine reported a "Get data timeout" error during execution.

   It is necessary to troubleshoot based on the specific situation, whether the request was intercepted by the gateway, or if there was an error in the execution of the engine on the other side. It could also be due to poor network conditions. If the issue is caused by poor network conditions, you can alleviate this error by modifying the relevant network configuration. Please refer to ``examples/opencore-demo/`` for configuration examples and :doc:`/reference/engine-config` for detailed settings.

**Q**: SCQL results from executing group by related syntax are incomplete or do not match the MySQL results?

   SCQL, to protect data privacy and prevent the malicious theft of data within groups, hides groups where the number of data items within a group is less than the GroupByThreshold. For specific details, please refer to ``examples/opencore-demo/`` for configuration examples and :doc:`/reference/engine-config` for the security_compromise.group_by_threshold configuration option (by default, groups with fewer than 4 data items are not displayed). Setting this value to 1 will disable the group filtering operation.

**Q**: There are precision errors in the numerical calculations.

   When SCQL enters secure MPC protocol, it needs to encode the data into Ring64 or Ring128 and then perform the secure computation. Numerical inaccuracies can occur during both the encoding and the secure computation processes, and this is unavoidable.

Configuration Issues
--------------------

**Q**: Data source configuration for different data sources.

   Please refer to the deployment documentation :doc:`/reference/engine-config`.

**Q**: How to configure relevant timeout settings when the network quality is poor.

   In a poor network environment, you can appropriately increase **link_recv_timeout_ms** (the waiting time for the receiving party) and decrease **link_throttle_window_size** (the size of the channel sliding window). You can also appropriately configure **http_max_payload_size** (the size of individual packets when splitting data for transmission) and **link_chunked_send_parallel_size** (the number of chunks sent in parallel).

   For specific configurations, please refer to the configuration documentation :doc:`/reference/engine-config`.
