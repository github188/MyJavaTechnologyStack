package com.sharpwisdom.third.secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by wuys on 2016/8/17.
 */
public class GroupingComparator extends WritableComparator {

    protected GroupingComparator(){
        super(IntPair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair a1 = (IntPair)a;
        IntPair b1 = (IntPair)b;
        return a1.getFirst().compareTo(b1.getFirst());
    }
}
