package com.kafka.client;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
    
    /**
     * The logic takes the key, which we expect to be the IP address, 
     * finds the last octet and does a modulo operation on the number of partitions defined within Kafka for the topic. 
     * The benefit of this partitioning logic is all web visits from the same source IP end up in the same Partition.
     *  Of course so do other IPs,but your consumer logic will need to know how to handle that.
     * 
     */
 
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }
 
}
