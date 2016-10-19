package com.sharpwisdom.third.secondarySort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class IntPair implements WritableComparable<IntPair> {

    private String first = "";
    private String second = "";

    public IntPair(){}

    public IntPair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        if(!this.first.equals(o.getFirst())){
            return this.first.compareTo(o.getFirst());
        }else{
            if(!this.second.equals(o.getSecond())){
                return this.second.compareTo(o.getSecond());
            }else{
                return 0;
            }
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(first);
        output.writeUTF(second);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.first = input.readUTF();
        this.second = input.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return this.first.equals(((IntPair) o).getFirst()) && this.second.equals(((IntPair) o).getSecond());
    }

    @Override
    public int hashCode() {
        return this.first.hashCode() * 133 + this.second.hashCode();
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }
}
