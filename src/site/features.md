# kafka-yarn-application

## features
* Able to configure a set of kafka brokers using YARN and have this information stored within zookeeper.
  Configuration includes resource requirements like minimum and maximum memory.
* Able to start these brokers using YARN. Uses information stored in zookeeper when the brokers were configured.
* Able to stop these brokers using YARN
* Able to query these brokers for topics and other information
* Able to create, delete topics on these brokers
