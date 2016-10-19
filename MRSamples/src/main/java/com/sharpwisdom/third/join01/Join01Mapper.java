package com.sharpwisdom.third.join01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 * 需求:获取员工所在部门信息,输出格式要求:员工编号,员工姓名,部门名称,部门编号
 * 1.原始数据
 * 员工数据
 * empno    ename   job     mgr     hiredate    sal     comm    deptno      loc
 * 7499     allen   saleman  7698   1981-02-20   1600    300     30
 * 7469     allen   saleman  7698   1981-02-20   1600    300     10
 * 7654     clark   managers 7639   1981-02-21   1250    1400    30         boston
 *
 * 部门数据
 * deptno   dname   loc
 * 30       sales   chicago
 * 20     research  dallas
 * 10    accounting  newyork
 *
 * 2.实现功能类似于:  select e.empno,e.ename,d.dname,d.deptno from emp e join dept d on e.deptno=d.deptno;
 * 3.处理join的思路:
 *          将Join key 当作map的输出key,也就是reduce的输入key,这样只要join的key相同,shuffle过后,就会进入到同一个reduce的key - value list中去
 *          需要为join的2张表设计一个通用的一个bean,并且bean中加一个flag的标志属性,这样可以根据flag来区分是哪张表的数据
 *          reduce阶段根据flag来判断是员工数据还是部门数据就很容易了.而join的真正处理是在reduce阶段.
 * 4.实现中间bean
 *          存储数据的bean(由于数据要在网络上传输必须序列化,hadoop处理时需要分组和排序，所以需要实现WritableComparable接口);
 *
 */
public class Join01Mapper extends Mapper<LongWritable,Text,IntWritable,Employee>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        if(arr.length <= 3){//部门数据
            Employee employee = new Employee();
            employee.setDeptno(arr[0]);
            employee.setDname(arr[1]);
            employee.setFlag(0);
            context.write(new IntWritable(Integer.valueOf(arr[0])),employee);
        }else{
            Employee employee = new Employee();
            employee.setEmpno(arr[0]);
            employee.setEname(arr[1]);
            employee.setDeptno(arr[7]);
            employee.setFlag(1);
            context.write(new IntWritable(Integer.valueOf(arr[7])),employee);
        }
    }
}
