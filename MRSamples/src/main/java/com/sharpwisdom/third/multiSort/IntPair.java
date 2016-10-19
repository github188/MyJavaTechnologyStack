package com.sharpwisdom.third.multiSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class IntPair implements WritableComparable<IntPair> {

    private String firstKey;
    private int secondKey;

    public IntPair(){}

    public IntPair(String firstKey, int secondKey) {
        this.firstKey = firstKey;
        this.secondKey = secondKey;
    }

    @Override
    public int compareTo(IntPair o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(firstKey);
        dataOutput.writeInt(secondKey);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.firstKey = dataInput.readUTF();
        this.secondKey = dataInput.readInt();
    }

    public String getFirstKey() {
        return firstKey;
    }

    public void setFirstKey(String firstKey) {
        this.firstKey = firstKey;
    }

    public int getSecondKey() {
        return secondKey;
    }

    public void setSecondKey(int secondKey) {
        this.secondKey = secondKey;
    }
}
