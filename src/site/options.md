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
### --start <start>.json
Start the configured brokers.  
### --monitor
Monitor the started brokers
### --status 
Status of the started brokers
### --stop 
Stop the running brokers
