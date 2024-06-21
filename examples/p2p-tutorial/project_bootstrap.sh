#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 ALICE_BROEKR_PORT BOB_BROEKR_PORT"
    exit 1
fi

ALICE_BROEKR_PORT=$1
BOB_BROEKR_PORT=$2

# Create project, invite party to join
./brokerctl create project --project-id "demo" --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl invite bob --project-id "demo" --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl process invitation 1 --response "accept" --project-id "demo" --host http://localhost:$BOB_BROEKR_PORT

# Create tables
./brokerctl create table ta --project-id "demo" --columns "ID string, credit_rank int, income int, age int" --ref-table alice.user_credit --db-type mysql --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl create table tb --project-id "demo" --columns "ID string, order_amount double, is_active int" --ref-table bob.user_stats --db-type mysql --host http://localhost:$BOB_BROEKR_PORT

# Grant CCL
./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name ID --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name income --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant alice PLAINTEXT --project-id "demo" --table-name ta --column-name age --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant bob PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name ta --column-name ID --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant bob PLAINTEXT_AFTER_GROUP_BY --project-id "demo" --table-name ta --column-name credit_rank --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant bob PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name ta --column-name income --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant bob PLAINTEXT_AFTER_COMPARE --project-id "demo" --table-name ta --column-name age --host http://localhost:$ALICE_BROEKR_PORT
./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name ID --host http://localhost:$BOB_BROEKR_PORT
./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:$BOB_BROEKR_PORT
./brokerctl grant bob PLAINTEXT --project-id "demo" --table-name tb --column-name is_active --host http://localhost:$BOB_BROEKR_PORT
./brokerctl grant alice PLAINTEXT_AFTER_JOIN --project-id "demo" --table-name tb --column-name ID --host http://localhost:$BOB_BROEKR_PORT
./brokerctl grant alice PLAINTEXT_AFTER_COMPARE --project-id "demo" --table-name tb --column-name is_active --host http://localhost:$BOB_BROEKR_PORT
./brokerctl grant alice PLAINTEXT_AFTER_AGGREGATE --project-id "demo" --table-name tb --column-name order_amount --host http://localhost:$BOB_BROEKR_PORT
