# How to use
- open three different terminals. One for each broker 
- run ``` go run main.go  -brokerId=1 -port=8080 -raftport=7070``` in terminal 1
- run ``` go run main.go  -brokerId=2 -port=8081 -raftport=7071``` in terminal 2
- run ``` go run main.go  -brokerId=3 -port=8082 -raftport=7072``` in terminal 3
- check which broker becomes the leader.
- Connect to that broker using telnet localhost portnumber
- Type any of the commands written below
## List of Commands for TCP Server
- CRTP topic_name [no. of partitions] 
- PUBS topic_name msg
- SUBS topic_name consumer_group [no. of partitions to subscribe to]  
- CONS topic_name subscription_id offset_value


#### Message Format
1) Example message ``` "{""Header"":""header"",""Key"":""124"",""Body"":""Hello World!""}" ```
2) A example of PUBS command will be: 
   ```PUBS topic_name "{""Header"":""header"",""Key"":""124"",""Body"":""Hello World!""}"```
3) Message should be enclosed in quotes
4) To preserve quotes inside curly braces it should be preceded by quotes
5) Not the best way to handle this but for now it will do


### Feature List

#### Features for Publisher
- [x] If the publisher does not provide a key, the message should be published to the partition in a round robin algorithm
- [x] if the publisher provides a key, hash the key and publish the message to the resultant partition   


#### Features for Consumers
- [x] Consumer should provide an offset value and receive all the messages from that offset to the end
- [x] Multiple Consumers belonging to the same Consumer Group cannot consume messages from the same partition
- [x] BUGS (Currently if a new consumer joins, all unassigned partitions are allocated.)

<!-- - [] if a new consumer is allocated and a previous consumers subscription is removed, notify the consumer -->

<!-- Index file
offset, index

log file
offset, message -->


#### File Manager 
- [x] Every Broker instance will create a folder with the name kafkaesque-id
- [x] Every partition will be created as sub folders with the name topicName-partitionNumber
- [x] Every partition folder will have two files, index file and log file 
- [x] Each log file has a size limit. After the size has reached a new log and index file are created

#### RAFT 
- [x] Create Two groups of Raft. One at the Broker Level (Leader Broker will keep track of active partitions and any changes that happen to the broker)
- [x] Another Raft group will be at the partition level. Each partition will create a new instance of Raft to communicate with replicated partitions
- [x] Implement Leader Election protocol to work for partitions
- [x] Implement Log replication for partitions
- [x] Storage for Raft (Managed using File Manager)

#### Service Discovery (Optional)
Currently Peers port numbers are hard coded in the config file 
- [ ] Copy Hashicorp serf library into our project which can be used for service discovery

