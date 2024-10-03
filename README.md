-----------PLS NOTE: This is a read.me file for another phase, changes need to be made-------------

#### Set up
```bash
docker-compose down
docker-compose up
```
---------------------------------------------------------------
#### Delete
```bash
# Stop all running containers
docker stop $(docker ps -q)

# Remove all containers
docker rm $(docker ps -aq)

# Optionally, remove all unused images, networks, and volumes
docker system prune -a

```bash
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker system prune -a
```
---------------------------------------------------------------

#### kafka cli command
```bash
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic time_series
docker-compose exec kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic time_series