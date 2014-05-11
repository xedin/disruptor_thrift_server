## LMAX Disruptor backed Thrift Server implementation.

This server is half-sync/half-async and uses separate invocation executor.

On 4 node cluster with SSDs and RF=3 Cassandra (trunk branch, stress -n 3000000, QUORUM read/write)
shows around __3x__ (!) improvement in read and __2x__ (!) improvement in write speed as well as 
significant latency improvements over THsHaServer and about __1.5x__ read/write throughput and
__2x__ latency improvement over TThreadedSelectorServer.

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at [http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cthrift-server).

Example for Maven:

```xml
<dependency>
    <groupId>com.thinkaurelius.thrift</groupId>
    <artifactId>thrift-server</artifactId>
    <version>0.3.0</version>
</dependency>
```
and for Ivy:

```xml
<dependency org="com.thinkaurelius.thrift" name="thrift-server" rev="0.3.5" />
```

## Performance numbers

### TDisruptorServer (1 write, 3 read runs):

#### WRITE (3,000,000 keys)
Averages from the middle 80% of values:  
interval_op_rate          : 19539  
interval_key_rate         : 19539  
latency median            : 1.9  
latency 95th percentile   : 5.4  
latency 99.9th percentile : 83.3  
Total operation time      : 00:02:45  

#### READ
Averages from the middle 80% of values:  
interval_op_rate          : 23836  
interval_key_rate         : 23836  
latency median            : 2.2  
latency 95th percentile   : 4.7  
latency 99.9th percentile : 21.7  
Total operation time      : 00:02:36  

Averages from the middle 80% of values:  
interval_op_rate          : 23530  
interval_key_rate         : 23530  
latency median            : 2.1  
latency 95th percentile   : 4.1  
latency 99.9th percentile : 23.1  
Total operation time      : 00:02:25  

Averages from the middle 80% of values:  
interval_op_rate          : 24217  
interval_key_rate         : 24217  
latency median            : 2.1  
latency 95th percentile   : 4.1  
latency 99.9th percentile : 24.2  
Total operation time      : 00:02:28  

  
### THsHaServer (1 write, 3 read runs):

#### WRITE (3,000,000 keys)
Averages from the middle 80% of values:  
interval_op_rate          : 11167  
interval_key_rate         : 11167  
latency median            : 4.1  
latency 95th percentile   : 6.3  
latency 99.9th percentile : 135.6  
Total operation time      : 00:04:34  

#### READ
Averages from the middle 80% of values:  
interval_op_rate          : 8176  
interval_key_rate         : 8176  
latency median            : 6.1  
latency 95th percentile   : 8.4  
latency 99.9th percentile : 27.2  
Total operation time      : 00:06:20  

Averages from the middle 80% of values:  
interval_op_rate          : 8106  
interval_key_rate         : 8106  
latency median            : 5.9  
latency 95th percentile   : 8.8  
latency 99.9th percentile : 29.3  
Total operation time      : 00:06:15  

Averages from the middle 80% of values:  
interval_op_rate          : 8133  
interval_key_rate         : 8133  
latency median            : 5.8  
latency 95th percentile   : 8.2  
latency 99.9th percentile : 26.5  
Total operation time      : 00:06:15