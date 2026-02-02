# Build a Postgres 15 image that includes our init script
FROM postgres:15

COPY init/create_dbs.sql /docker-entrypoint-initdb.d/01_create_dbs.sql

RUN chmod 644 /docker-entrypoint-initdb.d/01_create_dbs.sql
