# kafka-yarn-application

## Options
### --config 
### --start 
Start the configured brokers. 
### --monitor 
Monitor the started brokers.
### --status 
Status of the started brokers.
### --stop 
Stop the running brokers.
### Required: input.json
This json file specifies the zookeeper server used by the YARN application master as well as broker information 
An example json file:
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

