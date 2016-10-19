package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 */
public class SecondClass implements Writable {

    private String userName;
    private int classNum;

    public SecondClass(){}

    public SecondClass(String userName, int classNum) {
        this.userName = userName;
        this.classNum = classNum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userName);
        dataOutput.writeInt(classNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.userName = dataInput.readUTF();
        this.classNum = dataInput.readInt();
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public int getClassNum() {
        return classNum;
    }

    public void setClassNum(int classNum) {
        this.classNum = classNum;
    }

    @Override
    public String toString() {
        return "SecondClasst" + userName + "t" + classNum;
    }
}
