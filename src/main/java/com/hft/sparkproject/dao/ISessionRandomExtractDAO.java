package com.hft.sparkproject.dao;


import com.hft.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionRandomExtractDAO {

	/**
	 * 插入session随机抽取
	 * @param sessionAggrStat  sessionAggrStat
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
	
}
