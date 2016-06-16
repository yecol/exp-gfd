# Expertimental Code for GFD

### Compile

```bash
# make metis to partition data graphs
cd lib/metis
make

# package jars for main code
mvn package
```


### Run
```bash
# to partition graph
gpartition GRAPH_FILE K_PARTITION

# to run disVal or repVal in distributed setting

# 1) launch the coordinator
java -Djava.security.policy=security.policy -jar COORDINATOR.jar
# 2) launch and register worker(s) to the coordinator
java -Djava.security.policy=security.policy -jar WORKER.jar COORDINATOR_IP
# 3) launch client which sends query to the coordinator
java -Djava.security.policy=security.policy -jar grape-client-0.1.jar COORDINATOR_IP
```


### Data Format
- Query format: see queries files in data/query
- Data graph format: see original.dat in data. w.l.o.g. We converted it to numeric format and then processed by Metis, generated stuff presented in data. 
