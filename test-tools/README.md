# How to use ca_generator.sh

If you want to enable tls in SCQL but don't have existing CA files, ca_generator.sh can help to generate self-signed CA files.

``NOTE``: Self-signed CA files are ``not safe`` and can only be used for testing. Considering security, commercial CA files must be used in production environments.

## Step 1.

Run the command below to generate CA files:
```sh
bash ca_generator.sh
```

If the script completes successfully, you can obtain these CA files as follows:
```sh
└── tls
    ├── broker_alice-ca.crt
    ├── broker_alice-ca.key
    ├── broker_alice-ca.pem
    ├── broker_bob-ca.crt
    ├── broker_bob-ca.key
    ├── broker_bob-ca.pem
    ├── broker_carol-ca.crt
    ├── broker_carol-ca.key
    ├── broker_carol-ca.pem
    ├── engine_alice-ca.crt
    ├── engine_alice-ca.key
    ├── engine_alice-ca.pem
    ├── engine_bob-ca.crt
    ├── engine_bob-ca.key
    ├── engine_bob-ca.pem
    ├── engine_carol-ca.crt
    ├── engine_carol-ca.key
    ├── engine_carol-ca.pem
    ├── intermediate-ca.crt
    ├── intermediate-ca.key
    ├── intermediate-ca.pem
    ├── root-ca.crt
    ├── root-ca.key
    ├── root-ca.pem
    ├── scdb-ca.crt
    ├── scdb-ca.key
    └── scdb-ca.pem
```
* scdb-ca.crt and scdb-ca.key are files for SCDB
* broker_xxx-ca.crt and broker_xxx-ca.key are for SCQLBrokers
* engine_xxx-ca.crt and engine_xxx-ca.key are for SCQLEngines
* root-ca.* and intermediate-ca.* are files used for signing and you should save them carefully.

## Step 2.

Deploy CA files in your environments for SCQL to use:
> You need to add corresponding files in the environment of SCDB(SCQLBroker) and each SCQLEngine, and specify the relevant path in the configuration. For configuration details, refer to the [Document](https://www.secretflow.org.cn/docs/scql/en/development/scql_config_manual.html)

## Trouble shooting

The default generated CA files may not work well in your environment, if you encounter problems, please check the following:

- Script default add hosts ``localhost/scdb/engine_xxx/broker_xxx`` to ``subjectAltName``, so the generated CA can only be used for these hosts. please modifying if nodes' hosts do not matched.
  > e.g: If your scdb listens on IP xxx, you need to add ``IP.2 = xxx`` to ``[sans]`` in ca_generator.sh

- Auto generated root-ca.crt may not be trusted by default, try to use trusted CA as root-ca.crt and run the script again, please refer to the comments at the beginning of the ca_generator.sh for usage. If and only if in linux ``test environments``, you can optionally cp root-ca.crt to /etc/ssl/certs in the environments running scdbclient and scdb