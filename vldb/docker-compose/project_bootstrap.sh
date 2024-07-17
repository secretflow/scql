#!/bin/bash

# Create project, invite party to join
docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl create project --project-id "demo" --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl invite bob --project-id "demo" --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl process invitation 1 --response "accept" --project-id "demo" --host http://localhost:8080'


# Create tables
docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl create table ta --project-id "demo" --columns "ID string, credit_rank int, income int, age int" --ref-table alice.user_credit --db-type mysql --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl create table tb --project-id "demo" --columns "ID string, order_amount double, is_active int" --ref-table bob.user_stats --db-type mysql --host http://localhost:8080'


# Grant CCL
docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name ID --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name income --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name age --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name ta --column-name ID --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT_AFTER_GROUP_BY --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name ta --column-name income --host http://localhost:8080'

docker exec -it vldb-broker_alice-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT_AFTER_COMPARE --project-id "demo" --table-name ta --column-name age --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name ID --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name tb --column-name ID --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT_AFTER_GROUP_BY --project-id "demo" --table-name tb --column-name is_active --host http://localhost:8080'

docker exec -it vldb-broker_bob-1 bash -c '/home/admin/bin/brokerctl grant alice PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:8080'
