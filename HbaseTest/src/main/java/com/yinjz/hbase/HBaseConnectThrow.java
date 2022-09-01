package com.yinjz.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnectThrow {
    public static Connection connection = null;
    static {
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            System.out.println("连接获取失败");
            e.printStackTrace();
        }
    }
    /**
     * 连接关闭方法,用于进程关闭时调用
     * @throws IOException
     */
    public static void closeConnection() throws IOException { if (connection != null) {
        connection.close();
    }
    }


}
