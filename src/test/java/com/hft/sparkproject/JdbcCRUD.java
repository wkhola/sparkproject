package com.hft.sparkproject;

import org.junit.Test;

import java.sql.*;

public class JdbcCRUD {

    @Test
    public void insert(){

        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project?useUnicode=true&characterEncoding=utf8",
                    "root",
                    "123456");

            String sql = "insert into test_user(name, age) value (?, ?)";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, "张三");
            pstmt.setInt(2, 25);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try{
                if (pstmt != null){
                    pstmt.close();
                }
                if(conn != null){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}

