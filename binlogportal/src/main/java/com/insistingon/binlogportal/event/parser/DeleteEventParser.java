package com.insistingon.binlogportal.event.parser;

import cn.hutool.core.date.DateUtil;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.insistingon.binlogportal.BinlogPortalException;
import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.event.EventEntity;
import com.insistingon.binlogportal.event.EventEntityMode;
import com.insistingon.binlogportal.event.EventEntityType;
import com.insistingon.binlogportal.event.constants.CommonConstants;
import com.insistingon.binlogportal.event.parser.converter.CommonConverterProcessor;
import com.insistingon.binlogportal.event.parser.utils.StringUtils;
import com.insistingon.binlogportal.position.IPositionHandler;
import com.insistingon.binlogportal.tablemeta.TableMetaEntity;
import com.insistingon.binlogportal.tablemeta.TableMetaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Administrator
 */
public class DeleteEventParser implements IEventParser {
	private final Logger log = LoggerFactory.getLogger(UpdateEventParser.class);
	private CommonConverterProcessor commonConverterProcessor = new CommonConverterProcessor();

	private TableMetaFactory tableMetaFactory;
	private IPositionHandler positionHandler;
	private SyncConfig syncConfig;


	public DeleteEventParser(TableMetaFactory tableMetaFactory, IPositionHandler positionHandler) {
		this.tableMetaFactory = tableMetaFactory;
		this.syncConfig = tableMetaFactory.getSyncConfig();
		this.positionHandler = positionHandler;
	}

	@Override
	public List<EventEntity> parse(Event event) throws BinlogPortalException, SQLException {
		List<EventEntity> eventEntityList = new ArrayList<>();

		/**
		 * RBR 模式
		 */
		if (event.getData() instanceof DeleteRowsEventData) {
			DeleteRowsEventData deleteRowsEventData = event.getData();
			TableMetaEntity tableMetaEntity = tableMetaFactory.getTableMetaEntity(deleteRowsEventData.getTableId());
			log.debug("=====> 进入 [RBR-DELETE] 事件信息处理：[{}] <=====",Optional.ofNullable(tableMetaEntity).orElse(tableMetaEntity));

			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, tableMetaEntity)) {
				log.info("=====> 进入指定配置 [RBR-DELETE] 事件同步[tableMetaEntity] 表 [{}] 个 <=====", tableMetaEntity.toString());

				getRbrDeleteRowsEventData(event, eventEntityList, deleteRowsEventData, tableMetaEntity);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				log.info("=====> 进入全量配置 [RBR-DELETE] 事件同步[tableMetaEntity] 表 [{}] 个 <=====", tableMetaEntity.toString());

				getRbrDeleteRowsEventData(event, eventEntityList, deleteRowsEventData, tableMetaEntity);
			}
		}

		/**
		 * SBR 模式
		 */
		if (event.getData() instanceof QueryEventData) {
			QueryEventData queryEventData = event.getData();
			log.debug("=====> 进入 [SBR-DELETE] 事件信息处理：[{}] <=====",event.getData().toString());

			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, queryEventData) && queryVerifyDataById(queryEventData)) {
				getSbrEventEntityList(event, eventEntityList, queryEventData);
			}
			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig) && queryVerifyDataById(queryEventData)) {
				getSbrEventEntityList(event, eventEntityList, queryEventData);
			}
		}

		return eventEntityList;
	}

	private boolean queryVerifyDataById(QueryEventData queryEventData) throws BinlogPortalException {
		String key = String.format("%s-%s-%s-%s", queryEventData.getDatabase(), StringUtils.getTableName(queryEventData.getSql()), "sbr-delelte", StringUtils.getDataId(queryEventData.getSql()));
		log.info("=====> Redis.delete-key：{}] <=====", key);
		String value = positionHandler.getCacheObject(key);
		log.info("=====> Redis.delete-Value：[{}] <=====", value);
		if (Objects.isNull(value)) {
			log.info("=====> Entity.delete-Key：[{}] <=====", key);
			value = StringUtils.getTableName(queryEventData.getSql()).concat("-" + StringUtils.getDataId(queryEventData.getSql()));
			positionHandler.setCacheObject(key, value, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
			return true;
		}

		log.info("=====> [SBR-DELETE] 删除数据表 [{}] 与 数据ID [{}] 已执行过、跳出同步! <=====", StringUtils.getTableName(queryEventData.getSql()), StringUtils.getDataId(queryEventData.getSql()));
		return false;
	}

	private void getSbrEventEntityList(Event event, List<EventEntity> eventEntityList, QueryEventData queryEventData) {
		EventEntity eventEntity = new EventEntity();
		eventEntity.setEvent(event);
		eventEntity.setDatabaseName(queryEventData.getDatabase());
		eventEntity.setTableName(StringUtils.getTableName(queryEventData.getSql()));
		eventEntity.setDataId(StringUtils.getDataId(queryEventData.getSql()));
		eventEntity.setEventEntityType(EventEntityType.DELETE);
		eventEntity.setEventEntityMode(EventEntityMode.SBR);
		eventEntity.setSql(queryEventData.getSql());
		eventEntity.setSyncIdent(Long.valueOf(StringUtils.getDataId(queryEventData.getSql())));
		eventEntityList.add(eventEntity);
		log.debug("=====> [SBR-DELETE] 事件同步[eventEntity]实体 [{}] 个 <=====", eventEntityList.size());


	}

	private void getRbrDeleteRowsEventData(Event event, List<EventEntity> eventEntityList, DeleteRowsEventData deleteRowsEventData, TableMetaEntity tableMetaEntity) {
		List<Serializable[]> rows = deleteRowsEventData.getRows();
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
//			if (queryVerifyData(after, tableMetaEntity, columnData)) {
			if (queryVerifyDataOrigin(columnData)) {
				return;
			} else {
				columnData.put("data_origin", "02");
			}
			EventEntity eventEntity = new EventEntity();
			eventEntity.setEvent(event);
			eventEntity.setEventEntityType(EventEntityType.DELETE);
			eventEntity.setEventEntityMode(EventEntityMode.RBR);
			eventEntity.setDatabaseName(tableMetaEntity.getDbName());
			eventEntity.setTableName(tableMetaEntity.getTableName());
			eventEntity.setColumns(columns);
			eventEntity.setColumnsData(columnMetaDataList);
			eventEntity.setChangeAfter(changeAfter);
			eventEntity.setColumnData(columnData);
			eventEntity.setDataId(after[0]);
			eventEntity.setSyncIdent(Long.valueOf(after[0]));

			eventEntityList.add(eventEntity);
		});
		log.debug("=====> [RBR-DELETE] 事件同步[eventEntity] 实体 [{}] 个 <=====", eventEntityList.size());


	}

	/**
	 * 说明：TODO RBR 模式 根据时间字段缓存 删除 验证是否同步
	 *
	 * @param after
	 * @param eventEntity
	 * @param columnData
	 * @return boolean
	 * @date: 2022-8-15 14:09
	 * @throws:
	 */
	private boolean queryVerifyData(String[] after, TableMetaEntity eventEntity, Map<String, Object> columnData) {
		String key = eventEntity.getDbName().concat("-").concat(eventEntity.getTableName()).concat("-rbr-delete-").concat(after[0]);
		if (Objects.isNull(columnData)) {
			log.error("====> [RBR-DELETE] 同步字段数据为空！ <====");
			return true;
		}

		try {
			Long time = positionHandler.getCacheObject(key);
			String date = (String) Optional.ofNullable(columnData.get("update_time")).orElse(columnData.get("update_time"));
			if (!Objects.isNull(date)) {
				Long time1 = DateUtil.parse(date).getTime();
				positionHandler.setCacheObject(key, time1, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
				if (time1.equals(time)) {
					log.info("=====> [RBR-DELETE] 数据更新时间 [update_time] 字段 [{}]  数据更新时间 [{}] 与 缓存更新时间 [{}] 相同、本次不同步! <=====", date, time1, time);
					return true;
				}
			}
		} catch (BinlogPortalException e) {
			log.error("=====> [RBR-DELETE] 验证重复数据异常信息：[{}] ，异常原因：[{}]<=====", e.getMessage(), e.getCause().getMessage());
			return true;
		}
		return false;
	}

	/**
	 * 说明：TODO RBR 模式 【删除】 根据 [data_origin] 字段值 ，验证是否同步
	 *
	 * @param columnData
	 * @return boolean
	 * @date: 2022-8-15 13:11
	 * @throws:
	 */
	private boolean queryVerifyDataOrigin(Map<String, Object> columnData) {
		String origin = (String) Optional.ofNullable(columnData.get("data_origin")).orElse(columnData.get("data_origin"));
		if (!Objects.isNull(origin) && origin.equals(syncConfig.getDataOrigin())) {
			log.info("=====> [RBR-DELETE] 数据同步源字段 [data_origin] Value： [{}]  <=====", origin);
			return false;
		}
		log.info("=====> [RBR-DELETE] 数据同步源字段 [data_origin]" + (origin == null ? "为空" : "Value：[{}] ") + "， 同步数据：[{}]  <=====");

		return true;
	}
}
