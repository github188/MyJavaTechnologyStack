package com.sharpwisdom.third.join01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuys on 2016/8/16.
 */
public class Join01Reducer extends Reducer<IntWritable,Employee,NullWritable,Text>{

    @Override
    protected void reduce(IntWritable key, Iterable<Employee> values, Context context) throws IOException, InterruptedException {
        Employee dept = null;
        List<Employee> list = new ArrayList<>();
        for(Employee e:values){
            if(e.getFlag() == 0){
                dept = e;
            }else{
                list.add(e);
            }
        }

        for(Employee e:list){
            e.setDname(dept.getDname());
            context.write(NullWritable.get(), new Text(e.toString()));
        }
    }

    public static void main(String[] args) {
        Employee dept = null;
        Employee employee = new Employee("123","teste","456",null,1);
        Employee employee1 = new Employee("0123","eteste","3456",null,1);
        Employee employee2 = new Employee("1234","testee","4567","testd",0);
        List<Employee> values = new ArrayList<>();
        values.add(employee);
        values.add(employee1);
        values.add(employee2);
        List<Employee> list = new ArrayList<>();
        for(Employee e:values){
            if(e.getFlag() == 0){
                dept = e;
            }else{
                list.add(e);
            }
        }

        for(Employee e:list){
            e.setDname(dept.getDname());
            System.out.println(e.toString());
        }

    }
}
