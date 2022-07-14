package com.insistingon.binlogportal.event.parser.utils;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.tablemeta.TableMetaEntity;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Objects;

/**
 * @ClassName: StringUtils
 * @Description: TODO 字符串处理工具类
 * @Author: SuYang
 * @Date: 2022-7-12 15:55
 */

public class StringUtils extends org.apache.commons.lang.StringUtils {


	/**
	 * 说明：TODO 根据SQL 截取表名
	 *
	 * @param sql
	 * @return java.lang.String
	 * @date: 2022-7-12 15:57
	 */
	public static String getTableName(String sql) {
		if (org.apache.commons.lang.StringUtils.isBlank(sql)||!sql.contains(".")) {
			return sql;
		}
		sql = sql.substring(sql.indexOf(".") + 2);
		sql = sql.substring(0, sql.indexOf("`"));
		return sql;
	}

	public static boolean isDbAndTableNull(SyncConfig syncConfig) {
		if ((CollectionUtils.isEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isEmpty(syncConfig.getDataTables()))) {
			return true;
		}
		return false;
	}

	/**
	 * 说明：TODO SBR模式 指定数据库 和 表过滤验证
	 *
	 * @param queryEventData
	 * @return java.lang.Boolean
	 * @date: 2022-7-14 11:35
	 */
	public static Boolean isDbOrTableNull(SyncConfig syncConfig, QueryEventData queryEventData) {
		if ((CollectionUtils.isNotEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isNotEmpty(syncConfig.getDataTables()))) {
			if (syncConfig.getDatabaseName().contains(queryEventData.getDatabase()) && syncConfig.getDataTables().contains(getTableName(queryEventData.getSql()))) {
				return true;
			}
		}
		if ((CollectionUtils.isNotEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isEmpty(syncConfig.getDataTables()))) {
			if (syncConfig.getDatabaseName().contains(queryEventData.getDatabase())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 说明：TODO 指定数据库 和表过滤验证
	 *
	 * @param tableMetaEntity
	 * @return java.lang.Boolean
	 * @date: 2022-7-14 11:36
	 */
	public static Boolean isDbOrTableNull(SyncConfig syncConfig, TableMetaEntity tableMetaEntity) {
		if ((!Objects.isNull(tableMetaEntity) && CollectionUtils.isNotEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isNotEmpty(syncConfig.getDataTables()))) {
			if (syncConfig.getDatabaseName().contains(tableMetaEntity.getDbName()) && syncConfig.getDataTables().contains(getTableName(tableMetaEntity.getTableName()))) {
				return true;
			}
		}
		if ((!Objects.isNull(tableMetaEntity) && CollectionUtils.isNotEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isEmpty(syncConfig.getDataTables()))) {
			if (syncConfig.getDatabaseName().contains(tableMetaEntity.getDbName())) {
				return true;
			}
		}
		return false;
	}


}
