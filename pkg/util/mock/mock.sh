#!/bin/bash

python mock_from_testdata.py -t=grm -s="testdata/db_alice.json" -dgf="../../../.ci/docker-compose/scdb/conf_tmpl/toy_grm.json"
python mock_from_testdata.py -t=grm -s="testdata/db_bob.json" -dgf="../../../.ci/docker-compose/scdb/conf_tmpl/toy_grm.json"
python mock_from_testdata.py -t=grm -s="testdata/db_carol.json" -dgf="../../../.ci/docker-compose/scdb/conf_tmpl/toy_grm.json"


python mock_from_testdata.py -t=data -s="testdata/db_alice.json"
python mock_from_testdata.py -t=data -s="testdata/db_bob.json"
python mock_from_testdata.py -t=data -s="testdata/db_carol.json"


mv testdata/mysql_*_init.sql ../../../.ci/docker-compose/mysql/initdb/
mv testdata/postgres_*_init.sql ../../../.ci/docker-compose/postgres/initdb
mv testdata/*.csv ../../../.ci/docker-compose/csv

find . -type f -name '*.py' -print0 | xargs -0 black