## LMAX Disruptor backed Thrift Server implementation.

This server is half-sync/half-async and uses separate invocation executor.

On 2011 Macbook Pro in standard configuration with SSD, Cassandra (trunk branch, stress -n 3000000)
shows around __3x__ (!) improvement in read and __2x__ (!) improvement in write speed as well as 
significant latency improvements:

### TDisruptorServer (1 write, 3 read runs):

#### WRITE (3,000,000 keys)
Averages from the middle 80% of values:  
interval_op_rate          : 19539  
interval_key_rate         : 19539  
latency median            : 1.9  
latency 95th percentile   : 5.4  
latency 99.9th percentile : 83.3  
Total operation time      : 00:02:45  
_./tools/bin/cassandra-stress -n 3000000  142.00s user 81.63s system 133% cpu 2:47.56 total_  

#### READ
Averages from the middle 80% of values:  
interval_op_rate          : 23836  
interval_key_rate         : 23836  
latency median            : 2.2  
latency 95th percentile   : 4.7  
latency 99.9th percentile : 21.7  
Total operation time      : 00:02:36  
_./tools/bin/cassandra-stress -n 3000000 -o read  108.06s user 82.04s system 121% cpu 2:37.02 total_  
Averages from the middle 80% of values:  
interval_op_rate          : 23530  
interval_key_rate         : 23530  
latency median            : 2.1  
latency 95th percentile   : 4.1  
latency 99.9th percentile : 23.1  
Total operation time      : 00:02:25  
_./tools/bin/cassandra-stress -n 3000000 -o read  104.62s user 80.99s system 126% cpu 2:26.46 total_  
Averages from the middle 80% of values:  
interval_op_rate          : 24217  
interval_key_rate         : 24217  
latency median            : 2.1  
latency 95th percentile   : 4.1  
latency 99.9th percentile : 24.2  
Total operation time      : 00:02:28  
_./tools/bin/cassandra-stress -n 3000000 -o read  104.94s user 80.89s system 124% cpu 2:29.28 total_  
  
### THsHaServer (1 write, 3 read runs):

#### WRITE (3,000,000 keys)
Averages from the middle 80% of values:  
interval_op_rate          : 11167  
interval_key_rate         : 11167  
latency median            : 4.1  
latency 95th percentile   : 6.3  
latency 99.9th percentile : 135.6  
Total operation time      : 00:04:34  
_./tools/bin/cassandra-stress -n 3000000  140.63s user 80.20s system 79% cpu 4:36.63 total_  

#### READ
Averages from the middle 80% of values:  
interval_op_rate          : 8176  
interval_key_rate         : 8176  
latency median            : 6.1  
latency 95th percentile   : 8.4  
latency 99.9th percentile : 27.2  
Total operation time      : 00:06:20  
_./tools/bin/cassandra-stress -n 3000000 -o read  105.43s user 78.10s system 48% cpu 6:21.32 total_  
Averages from the middle 80% of values:  
interval_op_rate          : 8106  
interval_key_rate         : 8106  
latency median            : 5.9  
latency 95th percentile   : 8.8  
latency 99.9th percentile : 29.3  
Total operation time      : 00:06:15  
_./tools/bin/cassandra-stress -n 3000000 -o read  105.99s user 78.75s system 49% cpu 6:15.97 total_  
Averages from the middle 80% of values:  
interval_op_rate          : 8133  
interval_key_rate         : 8133  
latency median            : 5.8  
latency 95th percentile   : 8.2  
latency 99.9th percentile : 26.5  
Total operation time      : 00:06:15  
_./tools/bin/cassandra-stress -n 3000000 -o read  103.57s user 78.87s system 48% cpu 6:15.82 total_  