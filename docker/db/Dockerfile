FROM timescale/timescaledb-ha:pg13.16-ts2.15.3

ARG BUILDING_IOT_DB=buildingiot
ARG BUILDING_IOT_DB_USER=buildingiot
ARG BUILDING_IOT_DB_PASSWORD=buildingiotpassword

# Setup postgresql data dir and config
# RUN whoami
# RUN mkdir /data
# RUN cat /usr/share/postgresql/postgresql.conf.sample > /data/postgresql.conf

# RUN mkdir -p "$PGDATA" && chmod 700 "$PGDATA"

# PostgreSQL ENV variables
ENV POSTGRES_DB=$BUILDING_IOT_DB
ENV POSTGRES_USER=$BUILDING_IOT_DB_USER
ENV POSTGRES_PASSWORD=$BUILDING_IOT_DB_PASSWORD
ENV POSTGRES_HOST_AUTH_METHOD=password
# ENV PGDATA=/data/

COPY ./init.sql /docker-entrypoint-initdb.d/init.sql

# RUN chown -R buildingiot:buildingiot /home/postgres/pgdata/data

# VOLUME /data
EXPOSE 5432
