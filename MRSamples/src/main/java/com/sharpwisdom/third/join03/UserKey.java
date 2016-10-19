package com.sharpwisdom.third.join03;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class UserKey implements WritableComparable<UserKey> {

    private int cityNo;
    private boolean isPrimary;//true:city  false:user

    public UserKey(){}

    public UserKey(int cityNo, boolean isPrimary) {
        this.cityNo = cityNo;
        this.isPrimary = isPrimary;
    }

    @Override
    public int compareTo(UserKey o) {
        if(this.cityNo == o.getCityNo()){
            if(this.isPrimary == o.isPrimary()){
                return 0;
            }else{
                return this.isPrimary ? -1 : 1;//从小到大排序         comparator可以实现升序和降序
            }
        }else{
            return this.cityNo - o.getCityNo() > 0 ? 1 : -1;
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(cityNo);
        output.writeBoolean(isPrimary);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.cityNo = input.readInt();
        this.isPrimary = input.readBoolean();
    }

    @Override
    public int hashCode() {
        return this.cityNo;
    }

    public int getCityNo() {
        return cityNo;
    }

    public void setCityNo(int cityNo) {
        this.cityNo = cityNo;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setIsPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }
}
