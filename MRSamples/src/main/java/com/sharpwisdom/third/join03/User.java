package com.sharpwisdom.third.join03;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class User implements WritableComparable<User> {

    private String userNo = "";
    private String userName = "";
    private String cityNo = "";
    private String cityName = "";

    public User(){}

    public User(String userNo, String userName, String cityNo, String cityName) {
        this.userNo = userNo;
        this.userName = userName;
        this.cityNo = cityNo;
        this.cityName = cityName;
    }

    @Override
    public int compareTo(User o) {
        return 0;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(userNo);
        output.writeUTF(userName);
        output.writeUTF(cityNo);
        output.writeUTF(cityName);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.userNo = input.readUTF();
        this.userName = input.readUTF();
        this.cityNo = input.readUTF();
        this.cityName = input.readUTF();
    }

    @Override
    public String toString() {
        return userNo + "\t" + userName + "\t" + cityName;
    }

    public String getUserNo() {
        return userNo;
    }

    public void setUserNo(String userNo) {
        this.userNo = userNo;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getCityNo() {
        return cityNo;
    }

    public void setCityNo(String cityNo) {
        this.cityNo = cityNo;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }
}
