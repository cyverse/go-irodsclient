#! /bin/bash

docker-compose down
docker rm irods_test &> /dev/null
docker rm irods_db_test &> /dev/null