include .env

USER := $(shell whoami)
DOCKER_PROJ_NAME := ${DOCKER_PROJ_PREFIX}_${USER}


.PHONY: start-docker clean mock-data test analysis set-wan plot

default: all

start-docker:
	@(cd $(PWD)/docker-compose && python setup.py $(PWD)/.env)
	@(cd $(PWD)/docker-compose && docker compose -p ${DOCKER_PROJ_NAME} up -d)

clean:
	@(docker compose -p ${DOCKER_PROJ_NAME} down)
	@rm -rf logs

mock-data:
	python $(PWD)/scripts/mock_data.py -dd=$(PWD)/docker-compose/csv -s=$(PWD)/testdata/db.json

all: clean mock-data start-docker set-wan test analysis plot
	@echo "well done!"


test:
	@[ ! -d "$(PWD)/logs" ] && mkdir -p "$(PWD)/logs" && echo "Directory created." || echo "Directory already exists."
	go test -bench . -v -timeout=300m -args --conf=$(PWD)/docker-compose/regtest.yml --output_dir=$(PWD)/logs --container_names="${DOCKER_PROJ_NAME}-engine-alice-1,${DOCKER_PROJ_NAME}-engine-bob-1,${DOCKER_PROJ_NAME}-engine-carol-1"

analysis:
	@bash $(PWD)/scripts/analysis.sh ${DOCKER_PROJ_NAME}

set-wan:
	@bash $(PWD)/scripts/setup_wan.sh ${DOCKER_PROJ_NAME}-engine-alice-1 ${LATENCY} ${BANDWIDTH}
	@bash $(PWD)/scripts/setup_wan.sh ${DOCKER_PROJ_NAME}-engine-bob-1 ${LATENCY} ${BANDWIDTH}
	@bash $(PWD)/scripts/setup_wan.sh ${DOCKER_PROJ_NAME}-engine-carol-1 ${LATENCY} ${BANDWIDTH}

plot:
	@python $(PWD)/scripts/plot_csv_data.py $(PWD)/logs alice alice_op.csv ${DOCKER_PROJ_NAME}-engine-alice-1.csv
	@python $(PWD)/scripts/plot_csv_data.py $(PWD)/logs bob bob_op.csv ${DOCKER_PROJ_NAME}-engine-bob-1.csv
	@python $(PWD)/scripts/plot_csv_data.py $(PWD)/logs carol carol_op.csv ${DOCKER_PROJ_NAME}-engine-carol-1.csv