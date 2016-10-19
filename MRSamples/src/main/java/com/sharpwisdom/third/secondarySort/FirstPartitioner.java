package com.sharpwisdom.third.secondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wuys on 2016/8/17.
 */
public class FirstPartitioner extends Partitioner<IntPair,Text> {

    @Override
    public int getPartition(IntPair key, Text text, int numPartitions) {
        return Math.abs(key.hashCode() * 127 ) % numPartitions;
    }
}
