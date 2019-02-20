package com.hft.sparkproject.dao;


import com.hft.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDAO {

    /**
     * 根据主键查询任务
     * @param  taskid 主键
     * @return 任务
     */
    Task findById(Long taskid);

}
