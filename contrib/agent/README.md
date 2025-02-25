# SCQL Agent

SCQL Agent is designed to run SCQL task in Kuscia automatically.

## Steps to test locally

### 1. Prepare image
Run scripts to build image scql:latest if needed
```bash
bash docker/build.sh
```
### 2. Register image to kuscia nodes
Deploy kuscia cluster if not exists, and then register image to kuscia nodes.
```bash
# deploy kuscia cluster
mkdir kuscia && cd kuscia
# you can use specific kuscia image if needed
export KUSCIA_IMAGE=secretflow/kuscia:latest
docker pull $KUSCIA_IMAGE && docker run --rm $KUSCIA_IMAGE cat /home/kuscia/scripts/deploy/kuscia.sh > kuscia.sh && chmod u+x kuscia.sh
./kuscia.sh p2p

# register scql image to kuscia nodes
docker cp ${USER}-kuscia-autonomy-alice:/home/kuscia/scripts/tools/register_app_image .
cd ./register_app_image
# you can use specific scql image if needed, donot forget to modify the image infos in the end of file secretpad-scql-image.yaml
./register_app_image.sh -i scql:latest -n scql -f ../../scripts/kuscia-templates/secretpad-scql-image.yaml
```

### 3. Create Kuscia Job to run SCQL Agent

```bash
docker exec -it ${USER}-kuscia-autonomy-alice /bin/bash

# create job, you can modify the job details in "task_input_config"
export CTR_CERTS_ROOT=/home/kuscia/var/certs
curl -k -X POST 'https://localhost:8082/api/v1/job/create' \
 --header "Token: $(cat ${CTR_CERTS_ROOT}/token)" \
 --header 'Content-Type: application/json' \
 --cert ${CTR_CERTS_ROOT}/kusciaapi-server.crt \
 --key ${CTR_CERTS_ROOT}/kusciaapi-server.key \
 --cacert ${CTR_CERTS_ROOT}/ca.crt \
 -d '{
  "job_id": "job-alice-bob-002",
  "initiator": "alice",
  "max_parallelism": 1,
  "tasks": [
    {
      "task_id": "job-scql",
      "app_image": "scql-image",
      "parties": [
        {
          "domain_id": "alice",
          "role": "agent"
        },
        {
          "domain_id": "bob",
          "role": "agent"
        }
      ],
      "alias": "job-scql",
      "dependencies": [],
      "task_input_config": "{\"output_ids\": {\"alice\": \"alice-output-test1\"},\"initiator\":\"alice\",\"project_id\":\"projectid1\",\"query\":\"SELECT count(*) FROM ta join tb on ta.id1=tb.id2;\",\"tables\":{\"alice\":{\"tbls\":[{\"table_name\":\"ta\",\"ref_table\":\"alice-table\",\"db_type\":\"csvdb\",\"table_owner\":\"alice\",\"columns\":[{\"name\":\"id1\",\"dtype\":\"string\"},{\"name\":\"age\",\"dtype\":\"float\"}]}]},\"bob\":{\"tbls\":[{\"table_name\":\"tb\",\"ref_table\":\"bob-table\",\"db_type\":\"csvdb\",\"table_owner\":\"bob\",\"columns\":[{\"name\":\"id2\",\"dtype\":\"string\"},{\"name\":\"contact_cellular\",\"dtype\":\"float\"}]}]}},\"ccls\":{\"alice\":{\"column_control_list\":[{\"col\":{\"column_name\":\"id1\",\"table_name\":\"ta\"},\"party_code\":\"alice\",\"constraint\":\"PLAINTEXT\"},{\"col\":{\"column_name\":\"id1\",\"table_name\":\"ta\"},\"party_code\":\"bob\",\"constraint\":\"PLAINTEXT_AFTER_JOIN\"}]},\"bob\":{\"column_control_list\":[{\"col\":{\"column_name\":\"id2\",\"table_name\":\"tb\"},\"party_code\":\"bob\",\"constraint\":\"PLAINTEXT\"},{\"col\":{\"column_name\":\"id2\",\"table_name\":\"tb\"},\"party_code\":\"alice\",\"constraint\":\"PLAINTEXT_AFTER_JOIN\"}]}}}",
      "priority": 100
    }
  ]
}'

# check pods and container logs
kubectl get pods -A
crictl ps
crictl logs -f  xxx
```