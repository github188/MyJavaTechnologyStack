package com.sharpwisdom.third.join03;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by wuys on 2016/8/17.
 */
public class PKFKCompartor extends WritableComparator {

    protected PKFKCompartor(){
        super(UserKey.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        UserKey a1 = (UserKey) a;
        UserKey b1 = (UserKey) b;
        if(a1.getCityNo() == b1.getCityNo()){
            return 0;
        }else{
            return a1.getCityNo() > b1.getCityNo() ? 1 : -1;
        }
    }
}
