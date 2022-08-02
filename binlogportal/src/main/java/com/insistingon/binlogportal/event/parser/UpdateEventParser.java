package com.insistingon.binlogportal.event.parser;

import cn.hutool.core.date.DateUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.druid.DruidDSFactory;
import cn.hutool.setting.Setting;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
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

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Administrator
 */
public class UpdateEventParser implements IEventParser {
	private final Logger log = LoggerFactory.getLogger(UpdateEventParser.class);
	CommonConverterProcessor commonConverterProcessor = new CommonConverterProcessor();

	private TableMetaFactory tableMetaFactory;
	private IPositionHandler positionHandler;
	private SyncConfig syncConfig;
	private Setting setting;


	UpdateEventParser(TableMetaFactory tableMetaFactory, IPositionHandler positionHandler) {
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
			if (StringUtils.isDbOrTableNull(syncConfig, queryEventData) && queryDataById(queryEventData)) {
				getSbrUpdateEventEntity(event, eventEntityList, queryEventData);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig) && queryDataById(queryEventData)) {
				getSbrUpdateEventEntity(event, eventEntityList, queryEventData);
			}
		}

		log.debug("=====> [修改] 事件实体对象 [{}] 个 <=====",eventEntityList.size());

		return eventEntityList;
	}


	/**
	 * 说明：TODO SBR 模式验证数据同步
	 *
	 * @param queryEventData
	 * @return boolean
	 * @date: 2022-7-20 13:24
	 */
	private boolean queryDataById(QueryEventData queryEventData) throws SQLException, BinlogPortalException {
		DataSource source = DruidDSFactory.get(queryEventData.getDatabase());
		Entity entity = Db.use(source).queryOne(String.format("select * from %s where id=%s", StringUtils.getTableName(queryEventData.getSql()), StringUtils.getDataId(queryEventData.getSql())));

		String key = queryEventData.getDatabase().concat("-").concat(StringUtils.getTableName(queryEventData.getSql())).concat("-sbr-update-").concat(StringUtils.getDataId(queryEventData.getSql()));
		Long updateTime = Objects.isNull(entity.getStr("update_time")) == true ? null : DateUtil.parse(entity.getStr("update_time")).getTime();
		Long time = positionHandler.getCacheObject(key);
		log.debug("=====> Redis.update_time：{} <=====", time);
		if (!Objects.isNull(updateTime) && !updateTime.equals(time)) {
			log.debug("=====> Entity.update_time：{} <=====", updateTime);
			positionHandler.setCacheObject(key, updateTime, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
			return true;
		}

		log.info("=====> [SBR-UPDATE] 数据表更新时间 [update_time] 字段不存在 ，或 数据更新时间 [{}] 与 缓存更新时间 [{}] 相同、本次不同步! <=====", updateTime, time);
		return false;
	}


	private void getSbrUpdateEventEntity(Event event, List<EventEntity> eventEntityList, QueryEventData queryEventData) {

		EventEntity eventEntity = new EventEntity();
		eventEntity.setEvent(event);
		eventEntity.setDatabaseName(queryEventData.getDatabase());
		eventEntity.setTableName(StringUtils.getTableName(queryEventData.getSql()));
		eventEntity.setDataId(StringUtils.getDataId(queryEventData.getSql()));
		eventEntity.setEventEntityType(EventEntityType.UPDATE);
		eventEntity.setEventEntityMode(EventEntityMode.SBR);
		eventEntity.setSql(queryEventData.getSql());
		eventEntityList.add(eventEntity);
	}

	private void getRbrUpdateRowsEventData(Event event, List<EventEntity> eventEntityList, UpdateRowsEventData updateRowsEventData, TableMetaEntity tableMetaEntity) {

		List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
		rows.forEach(rowMap -> {
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
			if (queryVerifyData(after, tableMetaEntity, columnData)) {
				return;
			}
			EventEntity eventEntity = new EventEntity();
			eventEntity.setEvent(event);
			eventEntity.setDatabaseName(tableMetaEntity.getDbName());
			eventEntity.setTableName(tableMetaEntity.getTableName());
			eventEntity.setEventEntityType(EventEntityType.UPDATE);
			eventEntity.setEventEntityMode(EventEntityMode.RBR);
			eventEntity.setColumns(columns);
			eventEntity.setColumnsData(columnMetaDataList);
			eventEntity.setChangeBefore(changeBefore);
			eventEntity.setChangeAfter(changeAfter);
			eventEntity.setColumnData(columnData);
			eventEntity.setDataId(after[0]);

			eventEntityList.add(eventEntity);
		});
	}

	/**
	 * 说明：TODO  RBR 验证重复请求数据
	 *
	 * @param after
	 * @param eventEntity
	 * @param columnData
	 * @return boolean
	 * @date: 2022-7-21 16:27
	 */
	private boolean queryVerifyData(String[] after, TableMetaEntity eventEntity, Map<String, Object> columnData) {
		String key = eventEntity.getDbName().concat("-").concat(eventEntity.getTableName()).concat("-rbr-update-").concat(after[0]);
		if (Objects.isNull(columnData)) {
			log.error("====> [RBR-UPDATE] 同步字段数据为空！ <====");
			return true;
		}

		try {
			Long time = positionHandler.getCacheObject(key);
			String date = (String) Optional.ofNullable(columnData.get("update_time")).orElse(columnData.get("update_time"));
			if (!Objects.isNull(date)) {
				Long time1 = DateUtil.parse(date).getTime();
				positionHandler.setCacheObject(key, time1, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
				if (time1.equals(time)) {
					log.info("=====> [RBR-UPDATE] 数据更新时间 [update_time] 字段 [{}]  数据更新时间 [{}] 与 缓存更新时间 [{}] 相同、本次不同步! <=====", date, time1, time);
					return true;
				}
			}
		} catch (BinlogPortalException e) {
			log.error("=====> [RBR-UPDATE] 验证重复数据异常：[{}] ，异常原因：[{}]<=====", e.getMessage(),e.getCause().getMessage());
			return true;
		}
		return false;
	}
}
