package com.hft.spark.jdbc;

import com.hft.sparkproject.conf.ConfigurationManager;
import com.hft.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Administrator
 */
public class JDBCHelper {

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JDBCHelper instance = null;

    public static JDBCHelper getInstance(){
        if(instance == null){
            synchronized (JDBCHelper.class){
                if(instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> dataSource = new LinkedList<>();

    private JDBCHelper(){
        // 获取数据库连接池的大小
        int dataSourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        for (int i = 0; i < dataSourceSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                dataSource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    private synchronized Connection getConnection(){
        while (dataSource.size() == 0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }


    /*
     *
     * SQL  CRUD
     */

    public int executeUpdate(String sql, Object[] params){
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rtn = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(conn != null){
                dataSource.push(conn);
            }
        }
        return rtn;
    }

    public void executeQuery(String sql, Object[] params, QueryCallback callback){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null){
                dataSource.push(conn);
            }
        }

    }

    public interface QueryCallback{
        /**
         * @param rs
         */
        void process(ResultSet rs) throws Exception;
    }

    public int[] executeBatch(String sql, List<Object[]> paramsList){

        int[] rtn = null;

        Connection conn = null;
        PreparedStatement pstmt;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject( i + 1, params[i]);
                }
                pstmt.addBatch();
            }

            rtn = pstmt.executeBatch();

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null){
                dataSource.push(conn);
            }
        }

        return rtn;
    }
}
