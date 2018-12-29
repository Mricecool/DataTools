package com.datatools;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBUtils {

    //这里可以设置数据库名称
    private static String URL = "jdbc:sqlserver://localhost:1433;DatabaseName=CardCustomer";
    private static String USER = "sa";
    private static String PASSWORD = "mr.123";

    private static Connection conn = null;

    //静态代码块（将加载驱动、连接数据库放入静态块中）
    static {
        InputStream in = null;
        try {
            //1.加载驱动程序
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Properties properties = new Properties();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            in = DBUtils.class.getResourceAsStream("/config/config.properties");
            // 使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            URL = properties.getProperty("DB_URL");
            USER = properties.getProperty("USERNAME");
            PASSWORD = properties.getProperty("PASSWORD");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                    in = null;
                }
            } catch (Exception ex) {
            }
        }
    }

    //对外提供一个方法来获取数据库连接
    public static Connection getConnection() {
        try {
            //2.获得数据库的连接
            conn = (Connection) DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    //关闭数据库链接
    public static void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
