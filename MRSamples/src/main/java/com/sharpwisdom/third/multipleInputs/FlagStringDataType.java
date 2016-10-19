package com.sharpwisdom.third.multipleInputs;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class FlagStringDataType implements WritableComparable<FlagStringDataType> {

    private String value;
    private int flag;//0:phone,1:user

    public FlagStringDataType(){}

    public FlagStringDataType(String value, int flag) {
        this.value = value;
        this.flag = flag;
    }

    @Override
    public int compareTo(FlagStringDataType o) {
        if(this.flag >= o.getFlag()){
            if(this.flag > o.getFlag()){
                return 1;
            }
        }else{
            return -1;
        }
        return this.value.compareTo(o.getValue());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
