#! /bin/bash

docker-compose down
docker rm -f irods_test &> /dev/null
docker rm -f irods_db_test &> /dev/null