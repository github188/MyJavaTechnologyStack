package com.sharpwisdom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * Created by wuys on 2016/8/11.
 */
public class Test {

    private static final String HDFS_PATH = "hdfs://hadoop1:9000";
    private static final String DIR_PATH = "/test/output";
    private static final String FILE_PATH = "/test/input/test.log";

    public static void main(String[] args) throws Exception {
//        System.out.println(File.separator);
        System.setProperty("HADOOP_USER_NAME", "root");//这句话很重要，要不然会告你没有权限执行
        FileSystem fileSystem;
        fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
        //创建文件夹
//        makeDir(fileSystem);
        //删除文件夹
        deleteDir(fileSystem);
        //上传文件
//        uploadData(fileSystem);
        //下载文件
//      downloadData(fileSystem);
        //删除文件
//      deleteData(fileSystem);
    }

    /**
     * 删除文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void deleteData(FileSystem fileSystem) throws IOException {
        fileSystem.delete(new Path(FILE_PATH), true);
    }

    /**
     * 下载文件
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void downloadData(FileSystem fileSystem) throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));
        IOUtils.copyBytes(in, System.out, 1024, true);
    }

    /**
     * 创建文件夹
     *
     * @param fileSystem
     * @throws IOException
     */
    private static void makeDir(FileSystem fileSystem) throws IOException {
        fileSystem.mkdirs(new Path(DIR_PATH));
    }

    /**
     * 删除文件夹
     * @param fileSystem
     * @throws IOException
     */
    private static void deleteDir(FileSystem fileSystem) throws IOException{
        fileSystem.deleteOnExit(new Path(DIR_PATH));
    }


    /**
     * 上传文件
     *
     * @param fileSystem
     * @throws IOException
     * @throws FileNotFoundException
     */
    private static void uploadData(FileSystem fileSystem) throws IOException,
            FileNotFoundException {
        FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH));
        InputStream in = new FileInputStream(new File("d:/logs/test.log"));
        IOUtils.copyBytes(in, out, 1024, true);
    }
}
