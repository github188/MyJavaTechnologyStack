package com.sharpwisdom.third.multiSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by wuys on 2016/8/17.
 */
public class SelfPartitioner extends Partitioner<IntPair,IntWritable> {


    @Override
    public int getPartition(IntPair paramKEY, IntWritable paramVALUE, int numPartitions) {
        return Math.abs(paramKEY.getFirstKey().hashCode()*127) % numPartitions;
    }
}
