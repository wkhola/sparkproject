package com.hft.sparkproject.dao.impl;

import com.hft.sparkproject.dao.ITaskDAO;
import com.hft.sparkproject.domain.Task;
import com.hft.sparkproject.jdbc.JDBCHelper;


/**
 * 任务管理DAO实现类
 * @author Administrator
 */
public class TaskDAOImpl implements ITaskDAO {

    @Override
    public Task findById(Long taskid) {
        final Task task = new Task();

        String sql = "select * from task where task_id=?";
        Object[] params = new Object[]{taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, rs -> {
            if (rs.next()){
                long taskId = rs.getLong(1);
                String taskName = rs.getString(2);
                String createTime = rs.getString(3);
                String startTime = rs.getString(4);
                String finishTime = rs.getString(5);
                String taskType = rs.getString(6);
                String taskStatus = rs.getString(7);
                String taskParam = rs.getString(8);

                task.setTaskid(taskId);
                task.setTaskName(taskName);
                task.setCreateTime(createTime);
                task.setStartTime(startTime);
                task.setFinishTime(finishTime);
                task.setTaskType(taskType);
                task.setTaskStatus(taskStatus);
                task.setTaskParam(taskParam);
            }
        });
        return task;
    }
}
