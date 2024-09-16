# Distributed Execution of SQL Queries Using Trino


| Authors                                                                |
|------------------------------------------------------------------------|
| Apostolis Stamatis ([@apostolis1](https://github.com/apostolis1))      |
| Charalampos Botsas ([@harbots](https://github.com/harbots))           |
| Dimitrios Mitropoulos ([@dimitrismit](https://github.com/dimitrismit)) |


This file includes mostly the installation steps for the project.

Please see the included report for information regarding the benchmarks and the results.

## Installation

### Docker Installation


Postgres and Redis are running on docker containers in our setup. You can run the databases on system level, if you want.

A good guide on how to install docker can be found [here]( https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-16-04)

Our virtual machines are running on Ubuntu-16-04, make sure you follow a guid for your operating system.

### Trino Installation


Detailed instructions can be found on the [official docs](https://trino.io/docs/current/installation/deployment.html)

### Cassandra Installation


Installed cassandra 30x (to be compatible with python 3.5) following instructions from the official docs below (Installing the Debian packages)

Detailed instructions can be found on the [official docs](https://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html
)


Keep in mind that a bare metal installation of Cassandra (meaning without using containers) might install additional java versions, causing the trino launcher to break.

If this happens, you can use an environment variable when executing the trino launcher to specify the correct java version it should use.

After Cassandra is installed, you can start it with 

`sudo service cassandra start`

Then create the `trino` keyspace

`CREATE KEYSPACE IF NOT EXISTS trino WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };`



### PostgreSQL on docker

We are using the `postgres:15-bullseye` docker image due to a documented bug that prevents the `postgres:15` image from running

You can then connect to the running container to create the db and grant privileges

```
create database trino;
grant all privileges on database trino to postgres;
```



### Redis on docker

Create a config file with the specific configurations for your redis installation

Then simply start a redis container with a command similar to

`sudo docker run -d -v /home/user/local_data/:/data -v /home/user/redis.conf:/redis-stack.conf --name redis -p 6379:6379 redis/redis-stack-server:6.2.6-v9`

Make sure you replace the path to the backup volume and the config file

## Trino Connector files

Minimal configuration is required, you could use the templates from the book [Trino: The Definitive Guide](https://trino.io/trino-the-definitive-guide.html)

For reference, below you can find the ones we used

### postgresql.properties

```
connector.name=postgresql
connection-url=jdbc:postgresql://192.168.0.2:5432/trino
connection-user=postgres
connection-password=PASSWORD_HERE
```

### cassandra.properties

```
connector.name=cassandra
cassandra.contact-points=192.168.0.3
cassandra.load-policy.dc-aware.local-dc=datacenter1
cassandra.allow-drop-table=true
cassandra.batch-size=10
```

### redis.properties


```
connector.name=redis
redis.nodes=192.168.0.1:6379
redis.user=trino_usr
redis.password=PASSWORD_HERE
redis.default-schema=default
redis.key-prefix-schema-table=true
redis.table-description-dir=/home/user/redis_tables
redis.hide-internal-columns=false
```

