# InfluxDB metrics reporter for apache flume

To report metrics to InfluxDB, a flume agent must be started with this support. The Flume agent has to be started by passing in the following parameters as system properties prefixed by flume.monitoring., and can be specified in the flume-env.sh:

| Property Name | Default               |
| ------------- |:---------------------:|
| type          | ru.hh.flume.reporting.InfluxDBReporter |
| url           | http://localhost:8086 |
| database      | flume                 |
| username      | flume                 |
| password      | flume                 |
| retention     | autogent              |
| prefix        | -                     |
| tags          | -                     |

Example:
```
bin/flume-ng agent --conf conf --conf-file conf/example.conf --name a1 \
-Dflume.root.logger=INFO,console \
-Dflume.monitoring.type=ru.hh.flume.reporting.InfluxDBReporter \
-Dflume.monitoring.url=http://localhost:8086 \
-Dflume.monitoring.database=flume \
-Dflume.monitoring.username=flume \
-Dflume.monitoring.password=flume \
-Dflume.monitoring.retention=authogen \
-Dflume.monitoring.prefix=flume \
-Dflume.monitoring.tags=tag1=value1,tag2=value2
```
