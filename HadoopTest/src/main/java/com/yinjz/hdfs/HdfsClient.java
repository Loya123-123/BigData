package com.yinjz.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    // 1 文件上传
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://10.239.13.188:8020"), conf , "root");

//        // 2 执行上传API
        fs.copyFromLocalFile(new Path("/Users/loay/idea-workspace/BigData/data/input/words.txt"), new Path("/words.txt"));
        fs.printStatistics();
        // 3 关闭资源
        fs.close();
    }

    // 2 文件下载
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf , "root");

        // 2 执行下载操作
//		fs.copyToLocalFile(new Path("/banhua.txt"), new Path("e:/banhua.txt"));
        fs.copyToLocalFile(false, new Path("/banhua.txt"), new Path("e:/xiaohua.txt"), true);

        // 3 关闭资源
        fs.close();
    }


    // 3 文件删除
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf , "root");

        // 2 文件删除
        fs.delete(new Path("/0529"), true);

        // 3 关闭资源
        fs.close();
    }

    // 4 文件更名
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf , "root");

        // 2 执行更名操作
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));

        // 3 关闭资源
        fs.close();
    }

    // 5 文件详情查看
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), conf , "root");

        // 2 查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            // 查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());// 文件名称
            System.out.println(fileStatus.getPermission());// 文件权限
            System.out.println(fileStatus.getLen());// 文件长度

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("------班长分割线--------");
        }

        // 3 关闭资源
        fs.close();
    }


    // 6 判断是文件还是文件夹
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://10.239.13.188:9000"), conf , "root");

        // 2 判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            if (fileStatus.isFile()) {
                // 文件
                System.out.println("f:"+fileStatus.getPath().getName());
            }else{
                // 文件夹
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}

