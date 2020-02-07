package com.atguigu;

import java.sql.*;

public class KylinTest {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        //Kylin_JDBC 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://hadoop102:7070/first_project";

        //Kylin的用户名
        String KYLIN_USER = "ADMIN";

        //Kylin的密码
        String KYLIN_PASSWD = "KYLIN";

        //加载驱动
        Class.forName(KYLIN_DRIVER);

        //获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT deptno,sum(sal) FROM emp GROUP BY deptno");

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //打印结果
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "--" + resultSet.getDouble(2));
        }


    }

}
