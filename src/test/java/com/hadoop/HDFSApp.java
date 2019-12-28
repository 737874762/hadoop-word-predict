package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 〈使用java操作hdfs文件系统〉
 * <p>
 * 1.创建configuration
 * 2.获取FileSystem
 * 3. hdfs api操作
 */
public class HDFSApp {



    public static final String HDFS_PATH = "hdfs://192.168.199.200:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("---setup---");
        configuration = new Configuration();
//        设置副本数为1
        configuration.set("dfs.replication", "1");

//        configuration.set("dfs.client.use.datanode.hostname","true");


        /**
         * 参数1：hdfs的uri
         * 参数2：客户端指定的配置参数
         * 参数3：客户端的身份，就是操作用户名
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");

    }

    /**
     * 创建HDFS文件夹
     *
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        Path path = new Path("/hdfsapi/test");
        boolean mkdirs = fileSystem.mkdirs(path);
        System.out.println(mkdirs);
    }

    /**
     * 查看hdfs内容
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream open = fileSystem.open(new Path("/start-dfs.sh"));
        IOUtils.copyBytes(open, System.out, 1024);

    }

    /**
     * 创建hdfs文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.writeUTF("hello pk");
        out.flush();
        out.close();
    }

    /**
     * 测试文件名更改
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);

    }

    /**
     * 拷贝本地文件到HDFS文件系统
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("F:/桌面文件/2019-11-19至--/Hadoop考核/training-100000.txt");
        Path dst = new Path("/input_2017081119/training.data");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 拷贝大文件到HDFS文件系统：带进度
     */
    @Test
    public void copyFromLocalBigFile() throws Exception {

        InputStream in = new BufferedInputStream(new FileInputStream(new File("E:/IMOOC MV/博客/project.zip")));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/pro.tgz"),
                new Progressable() {
                    public void progress() {
                        System.out.print(".");//进度条
                    }
                });

        IOUtils.copyBytes(in, out, 4096);

    }

    /**
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/predict_output_2017081119/part-r-00000");
        Path dst = new Path("F:/桌面文件/2019-11-19至--/Hadoop考核/2017081119_预测结果.txt");
        fileSystem.copyToLocalFile(src, dst);
    }


    /**
     * 查看目标文件夹下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus file : statuses) {
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }

    }


    /**
     * 递归查看目标文件夹下的所有文件
     */
    @Test
    public void listFilesRecursive() throws Exception {

        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();


            System.out.println(isDir + "\t" + permission
                    + "\t" + replication + "\t" + length
                    + "\t" + path
            );
        }
    }


    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockLocations() throws Exception {

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/README.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for (BlockLocation block : blocks) {

            for (String name : block.getNames()) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength() + " : " + block.getHosts());
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/c.txt"), true);//true为是否递归删除
        System.out.println(result);
    }


    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("---tearDown---");
    }


}
