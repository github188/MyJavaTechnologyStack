package com.sharpwisdom.third.multiSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by wuys on 2016/8/17.
 */
public class GroupingComparator extends WritableComparator {

    protected GroupingComparator() {
        super(IntPair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair o1 = (IntPair) a;
        IntPair o2 = (IntPair) b;
        return o1.getFirstKey().compareTo(o2.getFirstKey());
    }
}
