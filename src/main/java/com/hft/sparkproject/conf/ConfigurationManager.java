package com.hft.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * @author Administrator
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("my.properties");

            prop.load(in);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key){
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public static Boolean getBoolean(String key){
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
