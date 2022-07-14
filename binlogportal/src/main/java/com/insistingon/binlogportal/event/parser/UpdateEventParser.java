package com.insistingon.binlogportal.event.parser;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
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
import java.util.*;

/**
 * @author Administrator
 */
public class UpdateEventParser implements IEventParser {

	CommonConverterProcessor commonConverterProcessor = new CommonConverterProcessor();

	private TableMetaFactory tableMetaFactory;

	private SyncConfig syncConfig;

	UpdateEventParser(TableMetaFactory tableMetaFactory) {
		this.tableMetaFactory = tableMetaFactory;
		this.syncConfig = tableMetaFactory.getSyncConfig();
	}

	@Override
	public List<EventEntity> parse(Event event) throws BinlogPortalException {
		List<EventEntity> eventEntityList = new ArrayList<>();

		/**
		 * RBR 模式
		 */
		if (event.getData() instanceof UpdateRowsEventData) {
			UpdateRowsEventData updateRowsEventData = event.getData();
			TableMetaEntity tableMetaEntity = tableMetaFactory.getTableMetaEntity(updateRowsEventData.getTableId());
			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, tableMetaEntity)) {
				getRbrUpdateRowsEventData(event, eventEntityList, updateRowsEventData, tableMetaEntity);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				getRbrUpdateRowsEventData(event, eventEntityList, updateRowsEventData, tableMetaEntity);
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
				getSbrUpdateEventEntity(event, eventEntityList, queryEventData);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				getSbrUpdateEventEntity(event, eventEntityList, queryEventData);
			}
		}


		return eventEntityList;
	}

	private void getSbrUpdateEventEntity(Event event, List<EventEntity> eventEntityList, QueryEventData queryEventData) {
		EventEntity eventEntity = new EventEntity();
		eventEntity.setEvent(event);
		eventEntity.setDatabaseName(queryEventData.getDatabase());
		eventEntity.setTableName(StringUtils.getTableName(queryEventData.getSql()));
		eventEntity.setEventEntityType(EventEntityType.UPDATE);
		eventEntity.setEventEntityMode(EventEntityMode.SBR);
		eventEntity.setSql(queryEventData.getSql());
		eventEntityList.add(eventEntity);
	}

	private void getRbrUpdateRowsEventData(Event event, List<EventEntity> eventEntityList, UpdateRowsEventData updateRowsEventData, TableMetaEntity tableMetaEntity) {
		List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
		rows.forEach(rowMap -> {
			EventEntity eventEntity = new EventEntity();
			eventEntity.setEvent(event);
			eventEntity.setDatabaseName(tableMetaEntity.getDbName());
			eventEntity.setTableName(tableMetaEntity.getTableName());
			eventEntity.setEventEntityType(EventEntityType.UPDATE);
			eventEntity.setEventEntityMode(EventEntityMode.RBR);
			List<TableMetaEntity.ColumnMetaData> columnMetaDataList = tableMetaEntity.getColumnMetaDataList();
			//解析update前后的数据
			String[] before = commonConverterProcessor.convertToString(rowMap.getKey(), columnMetaDataList);
			String[] after = commonConverterProcessor.convertToString(rowMap.getValue(), columnMetaDataList);

			List<String> columns = new ArrayList<>();
			List<Object> changeBefore = new ArrayList<>();
			List<Object> changeAfter = new ArrayList<>();
			Map<String, Object> columnData = new HashMap<>(10);

			for (int i = 0; i < before.length; i++) {
				columns.add(columnMetaDataList.get(i).getName());
				changeBefore.add(before[i]);
				changeAfter.add(after[i]);
				if (!Objects.equals(before[i], after[i])) {
					columnData.put(columnMetaDataList.get(i).getName(), after[i]);
				}
			}
			eventEntity.setColumns(columns);
			eventEntity.setColumnsData(columnMetaDataList);
			eventEntity.setChangeBefore(changeBefore);
			eventEntity.setChangeAfter(changeAfter);
			eventEntity.setColumnData(columnData);
			eventEntity.setDataId(after[0]);
			eventEntityList.add(eventEntity);
		});
	}
}
