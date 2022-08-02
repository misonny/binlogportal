package com.insistingon.binlogportal.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.event.Event;
import com.insistingon.binlogportal.tablemeta.TableMetaEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 事件实体，简化binlog事件，方便处理
 *
 * @author Administrator
 */
public class EventEntity {
	/**
	 * 保存原始binlog事件信息
	 */
	Event event;

	/**
	 * 事件类型
	 */
	EventEntityType eventEntityType;
	/**
	 * 事件模式
	 */
	EventEntityMode eventEntityMode;

	/**
	 * 数据库名称
	 */
	String databaseName;

	/**
	 * 数据表名称
	 */
	String tableName;

	/**
	 * SQL
	 */
	String sql;

	/**
	 * 数据ID
	 */
	String dataId;

	/**
	 * 新增、更新的字段与数据
	 */
	Map<String, Object> columnData;
	/**
	 * 元数据表字段名称集
	 */
	List<TableMetaEntity.ColumnMetaData> columnsData;

	/**
	 * 表字段
	 */
	List<String> columns;
	/**
	 * 变更前数据
	 */
	List<Object> changeBefore;

	/**
	 * 变更后数据
	 */
	List<Object> changeAfter;

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}

	public EventEntityType getEventEntityType() {
		return eventEntityType;
	}

	public void setEventEntityType(EventEntityType eventEntityType) {
		this.eventEntityType = eventEntityType;
	}

	public EventEntityMode getEventEntityMode() {
		return eventEntityMode;
	}

	public void setEventEntityMode(EventEntityMode eventEntityMode) {
		this.eventEntityMode = eventEntityMode;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getDataId() {
		return dataId;
	}

	public void setDataId(String dataId) {
		this.dataId = dataId;
	}

	public Map<String, Object> getColumnData() {
		return columnData;
	}

	public void setColumnData(Map<String, Object> columnData) {
		this.columnData = columnData;
	}

	public List<TableMetaEntity.ColumnMetaData> getColumnsData() {
		return columnsData;
	}

	public void setColumnsData(List<TableMetaEntity.ColumnMetaData> columnsData) {
		this.columnsData = columnsData;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<Object> getChangeBefore() {
		return changeBefore;
	}

	public void setChangeBefore(List<Object> changeBefore) {
		this.changeBefore = changeBefore;
	}

	public List<Object> getChangeAfter() {
		return changeAfter;
	}

	public void setChangeAfter(List<Object> changeAfter) {
		this.changeAfter = changeAfter;
	}

	@Override
	public String toString() {
		return "EventEntity{" +
				"event=" + event +
				", eventEntityType=" + eventEntityType +
				", eventEntityMode=" + eventEntityMode +
				", databaseName='" + databaseName + '\'' +
				", tableName='" + tableName + '\'' +
				", sql='" + sql + '\'' +
				", dataId='" + dataId + '\'' +
				", columnData=" + columnData +
				", columnsData=" + columnsData +
				", columns=" + columns +
				", changeBefore=" + changeBefore +
				", changeAfter=" + changeAfter +
				'}';
	}

	public String getJsonFormatData() {
		Map<String, Object> params = new HashMap<>();

		params.put("change_type", this.getEventEntityType().getDesc());
		params.put("change_mode", this.getEventEntityMode());
		Map<String, String[]> data = new HashMap<>();
		if (null != this.getColumnsData()) {

			for (int i = 0; i < this.getColumnsData().size(); i++) {
				String before = "";
				if (this.getChangeBefore() != null) {
					before = this.getChangeBefore().get(i) != null ? this.getChangeBefore().get(i).toString() : "";
				}
				String after = "";
				if (this.getChangeAfter() != null) {
					after = this.getChangeAfter().get(i) != null ? this.getChangeAfter().get(i).toString() : "";
				}
				String[] subData = new String[]{
						before,
						after,
						Objects.equals(before, after) ? "0" : "1"
				};

				data.put(this.getColumnsData().get(i).getName(), subData);
			}
		}
		params.put("change_data", data);
		params.put("database_name", this.getDatabaseName());
		params.put("column_name", this.getColumns());
		params.put("table_name", this.getTableName());
		params.put("sql", this.getSql());
		params.put("column_data", this.getColumnData());
		params.put("data_id", this.getDataId());
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
			return objectMapper.writeValueAsString(params);
		} catch (JsonProcessingException e) {
			return null;
		}
	}
}
