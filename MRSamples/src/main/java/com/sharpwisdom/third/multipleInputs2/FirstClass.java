package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/18.
 */
public class FirstClass implements Writable {

    private String value;

    public FirstClass(){}

    public FirstClass(String value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readUTF();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "FirstClasst" + value;
    }
}
