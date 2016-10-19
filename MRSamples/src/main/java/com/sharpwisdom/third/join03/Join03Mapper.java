package com.sharpwisdom.third.join03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 *
 * 1.原始数据
 * 人员ID     人员名称    地址ID
 * 1            张三      1
 * 2            李四      2
 * 3            王五      1
 *
 * 另一组为地址信息:
 * 地址ID     地址名称
 * 1            北京
 * 2            上海
 *
 * 1        张三
 * 1        北京
 * 1        王五
 *
 * 1.在join02中的reduce阶段是用一个list来保存了所有外键对应的所有人员信息，而List的最大值是Integer.MAX_VALUE，所以在数据量巨大的时候会造成List越界
 * 2.优化说明:
 *      结合第一种实现方式,第一种方式最需要改进的地方就是如果对于某个地址ID的迭带器values,如果values的第一个元素是地址信息的话，就没必要继续迭代了，因为
 *      下面的肯定都是人员信息，就可将人员的地址置为相应的地址.
 *
 *      mapreduce的partition和shuffle的过程,partitioner的主要功能是根据reduce的数量将map输出的结果进行分块，将数据送入到相应的reducer,
 *      所有的partitioner都必须实现partitioner接口并实现getPartition方法,该方法的返回值为int类型，并且取值范围是0-numOfReducer-1,
 *      从而能将map输出输入到相应的reducer中，对于某个mapreduce过程，Hadoop框架定义了默认的partitioner为HashPartition,该Partitioner使用key的
 *      hashCode来决定该key该输送到哪个reducer;shuffle将每个partitioner输出的结果根据key进行group及排序，将具有相同key的value构成一个values迭代器，并根据
 *      key进行排序分别调用开发者定义的reduce方法进行归并,从shuffle的过程我们可以看出key之间需要进行比较，通过比较才能知道某两个key是否相等或者进行排序，
 *      因此mapreduce所有的key必须实现comparable接口的compareto()方法从而实现这两个key之间的比较。
 *
 *      因此回到我们的问题,我们想要的是将地址信息在排序的过程中排到最前面，前面我们只通过locId进行比较的方法就不够用了，因为其无法标识出是地址表中的数据还是人员表
 *      中的数据，因此我们需要实现自己定义的Key数据结构，完成在想共同locId的情况下地址表更小的需求，由于map的中间结果需要写到磁盘上，因此必须实现象writeable接口，
 *      具体实现如下:
 *
 *
 */
public class Join03Mapper extends Mapper<LongWritable,Text,UserKey,User>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        if(arr.length == 2){//city
            User user = new User();
            user.setCityNo(arr[0]);
            user.setCityName(arr[1]);

            UserKey userKey = new UserKey();
            userKey.setCityNo(Integer.valueOf(arr[0]));
            userKey.setIsPrimary(true);
            context.write(userKey,user);
        }else{
            User user = new User();
            user.setUserNo(arr[0]);
            user.setUserName(arr[1]);
            user.setCityNo(arr[2]);

            UserKey userKey = new UserKey();
            userKey.setCityNo(Integer.valueOf(arr[2]));
            userKey.setIsPrimary(false);
            context.write(userKey,user);
        }
    }
}
