package com.hft.sparkproject.dao;

import com.hft.sparkproject.dao.impl.TaskDAOImpl;

/**
 * DAO 工厂类
 * @author Administrator
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return DAO
     */
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }
}
