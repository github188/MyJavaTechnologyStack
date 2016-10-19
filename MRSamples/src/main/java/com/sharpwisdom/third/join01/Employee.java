package com.sharpwisdom.third.join01;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 */
public class Employee implements WritableComparable<Employee>{

    private String empno = "";
    private String ename = "";
    private String deptno = "";
    private String dname = "";
    private int flag = 0;  //0表部门   1表示员工

    public Employee(){

    }

    public Employee(String empno, String ename, String deptno, String dname, int flag) {
        this.empno = empno;
        this.ename = ename;
        this.deptno = deptno;
        this.dname = dname;
        this.flag = flag;
    }

    public Employee(Employee employee){
        this.empno = employee.getEmpno();
        this.ename = employee.getEname();
        this.deptno = employee.getDeptno();
        this.dname = employee.getDname();
        this.flag = employee.getFlag();
    }

    @Override
    public int compareTo(Employee o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(empno);
        dataOutput.writeUTF(ename);
        dataOutput.writeUTF(deptno);
        dataOutput.writeUTF(dname);
        dataOutput.writeInt(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.empno = dataInput.readUTF();
        this.ename = dataInput.readUTF();
        this.deptno = dataInput.readUTF();
        this.dname = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.empno + "\t" + this.ename + "\t" + this.dname + "\t" + this.deptno;
    }

    public String getEmpno() {
        return empno;
    }

    public void setEmpno(String empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getDeptno() {
        return deptno;
    }

    public void setDeptno(String deptno) {
        this.deptno = deptno;
    }

    public String getDname() {
        return dname;
    }

    public void setDname(String dname) {
        this.dname = dname;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }
}
