package com.insistingon.binlogportal.tablemeta;

import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.BinlogPortalException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Administrator
 */
public class TableMetaFactory {

	SyncConfig syncConfig;

	public TableMetaFactory(SyncConfig syncConfig) {
		this.syncConfig = syncConfig;
	}

	public SyncConfig getSyncConfig() {
		return syncConfig;
	}

	public void setSyncConfig(SyncConfig syncConfig) {
		this.syncConfig = syncConfig;
	}

	/**
	 * 缓存tableId信息
	 */
	private Map<Long, TableMetaEntity> tableMetaEntityIdMap = new HashMap<>();

	/**
	 * 缓存表名信息
	 */
	private Map<String, TableMetaEntity> tableMetaEntityNameMap = new HashMap<>();

	public TableMetaEntity getTableMetaEntity(Long tableId, String dbName, String tableName) throws BinlogPortalException {
		Connection connection = null;
		try {
			if (tableMetaEntityIdMap.get(tableId) != null) {
				return tableMetaEntityIdMap.get(tableId);
			} else {
				String url = "jdbc:mysql://".concat(syncConfig.getHost()).concat(":") + syncConfig.getPort() + "/".concat(dbName).concat("?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8&allowLoadLocalInfile=true");
				connection = DriverManager.getConnection(url, syncConfig.getUserName(), syncConfig.getPassword());
				DatabaseMetaData dbmd = connection.getMetaData();
				ResultSet rs = dbmd.getColumns(dbName, dbName, tableName, null);
				TableMetaEntity tableMetaEntity = new TableMetaEntity();
				tableMetaEntity.setTableId(tableId);
				tableMetaEntity.setDbName(dbName);
				tableMetaEntity.setTableName(tableName);
				while (rs.next()) {
					TableMetaEntity.ColumnMetaData columnMetaData = new TableMetaEntity.ColumnMetaData();
					String colName = rs.getString("COLUMN_NAME");
					columnMetaData.setName(colName);
					String dbType = rs.getString("TYPE_NAME");
					columnMetaData.setType(dbType);
					tableMetaEntity.getColumnMetaDataList().add(columnMetaData);
				}
				tableMetaEntityIdMap.put(tableId, tableMetaEntity);
				return tableMetaEntity;
			}
		} catch (Throwable e) {
			throw new BinlogPortalException(e.getCause());
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	/**
	 * 根据tableId获取表元数据
	 *
	 * @param tableId
	 * @return
	 */
	public TableMetaEntity getTableMetaEntity(Long tableId) {
		return tableMetaEntityIdMap.get(tableId);
	}
}
