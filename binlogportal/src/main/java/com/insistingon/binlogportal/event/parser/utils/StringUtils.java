package com.insistingon.binlogportal.event.parser.utils;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.event.EventEntityType;
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
	 * @param SQL
	 * @return java.lang.String
	 * @date: 2022-7-12 15:57
	 */
	public static String getTableName(String SQL) {
		if (!StringUtils.isBlank(SQL) && !SQL.contains("`.`")) {
			if (EventEntityType.isCrudEventType(SQL.substring(0, 6))) {
				SQL = SQL.substring(SQL.indexOf("`") + 1);
				SQL = org.apache.commons.lang.StringUtils.substringBefore(SQL, "`");
				return SQL;
			}

		}
		if (!StringUtils.isBlank(SQL) && SQL.contains("`.`")) {
			SQL = SQL.substring(SQL.indexOf("`.`") + 3);
			SQL = SQL.substring(0, SQL.indexOf("`"));
			return SQL;
		}
		return SQL;
	}

	/**
	 * 说明：TODO 根据SQL 截取数据ID
	 * @param SQL
	 * @return java.lang.String
	 * @date: 2022-7-15 12:02
	 */
	public static String getDataId(String SQL) {
		if (!org.apache.commons.lang.StringUtils.isBlank(SQL)) {
			String EventType = SQL.substring(0, 6);
			if (EventEntityType.isUdEventType(EventType)) {
				SQL = org.apache.commons.lang.StringUtils.substringAfterLast(SQL,"=").trim();
				if (SQL.contains("'")) {
					SQL = org.apache.commons.lang.StringUtils.substring(SQL, 1, SQL.lastIndexOf("'"));
				}
			}

			if (EventEntityType.isInertEventType(EventType)) {
				SQL = org.apache.commons.lang.StringUtils.substringAfter(SQL, "VALUES");
				SQL = org.apache.commons.lang.StringUtils.substring(SQL, SQL.indexOf("(") + 1, SQL.indexOf(","));
				if (SQL.contains("'")) {
					SQL = org.apache.commons.lang.StringUtils.substring(SQL, 1, SQL.lastIndexOf("'"));
				}
				return SQL;
			}
			return SQL;
		}

		return SQL;
	}

	public static boolean isDbAndTableNull(SyncConfig syncConfig) {
		if ((CollectionUtils.isEmpty(syncConfig.getDatabaseName()) && CollectionUtils.isEmpty(syncConfig.getDataTables()))) {
			return true;
		}
		return false;
	}

	public static Boolean isDbInclude(String database){

		return false;
	}
	/**
	 * 说明：TODO  指定数据库 和 表过滤验证
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
//			if (syncConfig.getDatabaseName().contains(tableMetaEntity.getDbName()) && syncConfig.getDataTables().contains(getTableName(tableMetaEntity.getTableName()))) {
//				return true;
//			}
			if (syncConfig.getDataTables().contains(getTableName(tableMetaEntity.getTableName()))) {
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
