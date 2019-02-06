package com.hft.sparkproject;

import com.hft.sparkproject.jdbc.JDBCHelper;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCHelpTest {

    private JDBCHelper jdbcHelper = null;

    @Before
    public void setUp(){
        jdbcHelper = JDBCHelper.getInstance();
    }

    @Test
    public void jdbcHelpUpdateTest(){

        jdbcHelper.executeUpdate("insert into test_user(name, age) values(?,?)",
                new Object[]{"王二", 28});
    }

    @Test
    public void JdbcHelpQueryTest(){
        final Map<String, Object> testUser = new HashMap<>();

        jdbcHelper.executeQuery("select name, age from test_user where id = ?",
                new Object[]{1},
                rs -> {
                    if (rs.next()){
                        String name = rs.getString(1);
                        int age = rs.getInt(2);

                        testUser.put("name", name);
                        testUser.put("age", age);
                    }
                });

        System.out.println(testUser.get("name") + ":" + testUser.get("age"));
    }

    @Test
    public void JdbcBatchTest(){
        String sql = "insert into test_user(name, age) values(?,?)";

        List<Object[]> paramsList = new ArrayList<>();
        paramsList.add(new Object[]{"麻子", 30});
        paramsList.add(new Object[]{"王五", 35});
        jdbcHelper.executeBatch(sql, paramsList);
    }

}
