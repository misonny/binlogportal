package com.insistingon.binlogportal.event.parser;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.insistingon.binlogportal.BinlogPortalException;
import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.event.EventEntity;
import com.insistingon.binlogportal.event.EventEntityMode;
import com.insistingon.binlogportal.event.EventEntityType;
import com.insistingon.binlogportal.event.parser.converter.CommonConverterProcessor;
import com.insistingon.binlogportal.event.parser.utils.StringUtils;
import com.insistingon.binlogportal.tablemeta.TableMetaEntity;
import com.insistingon.binlogportal.tablemeta.TableMetaFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Administrator
 */
public class InsertEventParser implements IEventParser {
	CommonConverterProcessor commonConverterProcessor = new CommonConverterProcessor();

	private TableMetaFactory tableMetaFactory;
	private SyncConfig syncConfig;

	public InsertEventParser(TableMetaFactory tableMetaFactory) {
		this.tableMetaFactory = tableMetaFactory;
		this.syncConfig = tableMetaFactory.getSyncConfig();
	}

	@Override
	public List<EventEntity> parse(Event event) throws BinlogPortalException {
		List<EventEntity> eventEntityList = new ArrayList<>();

		/**
		 * RBR 模式
		 */
		if (event.getData() instanceof WriteRowsEventData) {
			WriteRowsEventData writeRowsEventData = event.getData();
			TableMetaEntity tableMetaEntity = tableMetaFactory.getTableMetaEntity(writeRowsEventData.getTableId());
			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, tableMetaEntity)) {
				getRbrWriteRowsEventData(event, eventEntityList, writeRowsEventData, tableMetaEntity);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				getRbrWriteRowsEventData(event, eventEntityList, writeRowsEventData, tableMetaEntity);
			}
		}

		/**
		 * SBR 模式
		 */
		if (event.getData() instanceof QueryEventData) {
			QueryEventData queryEventData = event.getData();
			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, queryEventData)) {
				getSbrWriteEventEntity(event, eventEntityList, queryEventData);
			}
			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				getSbrWriteEventEntity(event, eventEntityList, queryEventData);
			}
		}

		return eventEntityList;
	}

	private void getSbrWriteEventEntity(Event event, List<EventEntity> eventEntityList, QueryEventData queryEventData) {
		EventEntity eventEntity = new EventEntity();
		eventEntity.setEvent(event);
		eventEntity.setDatabaseName(queryEventData.getDatabase());
		eventEntity.setTableName(StringUtils.getTableName(queryEventData.getSql()));
		eventEntity.setEventEntityType(EventEntityType.INSERT);
		eventEntity.setEventEntityMode(EventEntityMode.SBR);
		eventEntity.setSql(queryEventData.getSql());
		eventEntityList.add(eventEntity);
	}

	private void getRbrWriteRowsEventData(Event event, List<EventEntity> eventEntityList, WriteRowsEventData writeRowsEventData, TableMetaEntity tableMetaEntity) {
		List<Serializable[]> rows = writeRowsEventData.getRows();
		rows.forEach(rowMap -> {
			List<TableMetaEntity.ColumnMetaData> columnMetaDataList = tableMetaEntity.getColumnMetaDataList();
			String[] after = commonConverterProcessor.convertToString(rowMap, columnMetaDataList);
			List<String> columns = new ArrayList<>();
			List<Object> changeAfter = new ArrayList<>();
			Map<String, Object> columnData = new HashMap<>(10);

			for (int i = 0; i < after.length; i++) {
				columns.add(columnMetaDataList.get(i).getName());
				changeAfter.add(after[i]);
				columnData.put(columnMetaDataList.get(i).getName(), after[i]);
			}

			EventEntity eventEntity = new EventEntity();
			eventEntity.setEvent(event);
			eventEntity.setEventEntityType(EventEntityType.INSERT);
			eventEntity.setDatabaseName(tableMetaEntity.getDbName());
			eventEntity.setTableName(tableMetaEntity.getTableName());
			eventEntity.setColumns(columns);
			eventEntity.setColumnsData(columnMetaDataList);
			eventEntity.setChangeAfter(changeAfter);
			eventEntity.setColumnData(columnData);
			eventEntityList.add(eventEntity);
		});
	}
}
