package com.hft.sparkproject;

import com.hft.sparkproject.conf.ConfigurationManager;
import org.junit.Test;

public class ConfigurationManagerTest {

    @Test
    public void test(){
        String aaa = ConfigurationManager.getProperty("aaa");
        String bbb = ConfigurationManager.getProperty("bbb");
        System.out.println(aaa);
        System.out.println(bbb);
    }


}
