# kafka-yarn-application

## Options
### --config <configure>.json
Provide a configuration that is used to configure the brokers across the cluster. An example configuration file:
<pre>
{
master: {
  zookeeper: {
  host: "localhost",
  port: 2181
  }
},
brokers: [
  {
    id: 0,
    port: 9092,
    zookeepers: [
      {
        host: "localhost",
        port: 2181
      }
    ] 
  }
]
}
</pre>
### --start zookeeper
Start the configured brokers. The AppMaster uses zookeeper to retrieve state of broker locations. Default zookeeper is localhost:2181
### --monitor zookeeper
Monitor the started brokers. The AppMaster uses zookeeper to retrieve state of broker locations. Default zookeeper is localhost:2181
### --status zookeeper
Status of the started brokers. The AppMaster uses zookeeper to retrieve state of broker locations. Default zookeeper is localhost:2181
### --stop zookeeper
Stop the running brokers. The AppMaster uses zookeeper to retrieve state of broker locations. Default zookeeper is localhost:2181
