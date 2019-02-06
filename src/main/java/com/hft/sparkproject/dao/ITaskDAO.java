package com.hft.sparkproject.dao;

import com.hft.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * @author : kai.wu
 * @date : 2019/2/6
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     * @param taskid 主键
     * @return 任务
     */
    Task findById(long taskid);
}
