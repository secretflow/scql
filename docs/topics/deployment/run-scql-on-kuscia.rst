==================
Run SCQL on Kusica
==================

SCQL can run on `Kuscia <https://github.com/secretflow/kuscia>`_. Deploying and running on Kuscia has the following advantages:

1. Kuscia is a k8s-based privacy-preserving computing task orchestration framework, it can simplify the deployment process, no need to manually exchange public keys between participating parties during deployment, and making scaling up or down much easier.
2. SCQL utilizes kuscia jobs for scheduling engines to handle query tasks. Kuscia jobs ensure isolation in the processing of different query tasks, and resources (cpu/mem/...) would be freed upon the completion of kuscia job, thereby boosting both stability and resource efficiency.
3. SCQL supports integration with Kuscia Datamesh API, enabling access to the data source information in Kuscia Datamesh.


The following section will describe how to deploy SCQL on Kuscia.

Only the SCQL P2P mode is compatible with Kuscia. SCQLBroker is deployed as resident service via `KusciaDeployment <https://www.secretflow.org.cn/zh-CN/docs/kuscia/v0.7.0b0/reference/concepts/kusciadeployment_cn>`_.

Prerequisites
=============

If you have not installed Kuscia yet, please refer to the following docs to complete the installation.

- `Kuscia quickstart on single-node <https://www.secretflow.org.cn/zh-CN/docs/kuscia/v0.7.0b0/getting_started/quickstart_cn>`_
- `Kuscia Multi-Node Deployment <https://www.secretflow.org.cn/zh-CN/docs/kuscia/v0.7.0b0/deployment/Docker_deployment_kuscia/deploy_p2p_cn>`_


Prepare SCQL AppImage
=====================

`AppImage <https://www.secretflow.org.cn/zh-CN/docs/kuscia/v0.7.0b0/reference/concepts/appimage_cn>`_ is a fundamental concept in Kuscia, it contains the app deployment configuration and image information. The following YAML file is an example SCQL AppImage template.

.. literalinclude:: ../../../scripts/kuscia-templates/scql-image.yaml
    :language: yaml

In the above AppImage template:

- ``.spec.configTemplates.brokerConf``: Its value is the configuration file content for SCQLBroker.

  -  NOTE: Placeholders in the form of ``{{.VARIABLE_NAME}}`` is kuscia built-in variables, which will be assigned values by kuscia when starting pods.
  -  Please modify the database connection configuration items ``storage.type`` and ``storage.conn_str`` to the actual db you wish to use. The example use sqlite.

- ``.spec.configTemplates.engineConf``: Its value is the configuration file content for SCQLEngine.

  - If you want to use Kuscia Datamesh as SCQL datasource router, please modify ``--datasource_router`` to ``kusciadatamesh``. Otherwise, fill the configuration item ``--embed_router_conf``.

- ``spec.image``: SCQL image information.

  - ``spec.image.name``: SCQL image name
  - ``spec.image.tag``: The tag you want to use.


Prepare SCQLBroker Deployment YAML
==================================

SCQLBroker KusciaDeployment yaml file looks as follows.

.. code-block:: yaml
    :emphasize-lines: 9,13

    apiVersion: kuscia.secretflow/v1alpha1
    kind: KusciaDeployment
    metadata:
      labels:
        kuscia.secretflow/app-type: scql
      name: scql
      namespace: cross-domain
    spec:
      initiator: alice # modify it to actual domain ID
      inputConfig: ""
      parties:
        - appImageRef: scql
          domainID: alice # make it same with `spec.initiator`
          role: broker

Please modify ``.spec.initiator`` and ``.spec.parties[0].domainID`` to the actual DomainID(DomainID is a Kuscia concept) you wish to deploy.


Let's go and deploy it
======================

1. Create AppImage

.. code-block:: bash

    # in kuscia autonomy (or kuscia lite) container
    # assume you have prepared scql AppImage file "scql-image.yaml"
    kubectl apply -f scql-image.yaml

    # check AppImage status
    kubectl get appimage scql

2. Create SCQLBroker KusciaDeployment

.. code-block:: bash

    # in kuscia autonomy (or kuscia lite) container
    # assume you have prepared SCQLBroker deployment file "broker-deploy.yaml"
    kubectl apply -f broker-deploy.yaml

    # check deploy status
    # pod status becoming `Running` indicates deployment was successful.
    kubectl get po -A


.. note::
    This article primarily discusses the YAML configuration for SCQL AppImage and SCQLBroker Deployment.
    Please read the tutorial `Run SCQL on Kuscia <https://www.secretflow.org.cn/zh-CN/docs/kuscia/main/tutorial/run_scql_on_kuscia_cn>`_ from kuscia's perspective for more detailed step-by-step instructions.