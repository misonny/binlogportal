package com.insistingon.binlogportal.event.parser;

import cn.hutool.core.date.DateUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.druid.DruidDSFactory;
import cn.hutool.setting.Setting;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
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
public class InsertEventParser implements IEventParser {
	private final Logger log = LoggerFactory.getLogger(UpdateEventParser.class);
	CommonConverterProcessor commonConverterProcessor = new CommonConverterProcessor();

	private TableMetaFactory tableMetaFactory;
	private IPositionHandler positionHandler;
	private SyncConfig syncConfig;
	private Setting setting;


	public InsertEventParser(TableMetaFactory tableMetaFactory, IPositionHandler positionHandler) {
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
		if (event.getData() instanceof WriteRowsEventData) {
			WriteRowsEventData writeRowsEventData = event.getData();
			TableMetaEntity tableMetaEntity = tableMetaFactory.getTableMetaEntity(writeRowsEventData.getTableId());

			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, tableMetaEntity)) {
				log.debug("=====> 进入[RBR-INSERT] 指定配置 事件同步[tableMetaEntity] 表信息 [{}] <=====", tableMetaEntity.toString());

				getRbrWriteRowsEventData(event, eventEntityList, writeRowsEventData, tableMetaEntity);
			}

			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig)) {
				log.debug("=====> 进入 [RBR-INSERT] 全量配置 事件同步[tableMetaEntity] 表信息 [{}] <=====", tableMetaEntity.toString());

				getRbrWriteRowsEventData(event, eventEntityList, writeRowsEventData, tableMetaEntity);
			}
		}

		/**
		 * SBR 模式
		 */
		if (event.getData() instanceof QueryEventData) {
			QueryEventData queryEventData = event.getData();
			log.debug("=====> 进入 [SBR-INSERT] 事件信息处理：[{}] <=====",event.getData().toString());

			/**
			 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
			 */
			if (StringUtils.isDbOrTableNull(syncConfig, queryEventData) && queryVerifyDataById(queryEventData)) {
				getSbrWriteEventEntity(event, eventEntityList, queryEventData);
			}
			/**
			 * 如未配置指定库和表名则所有同步
			 */
			if (StringUtils.isDbAndTableNull(syncConfig) && queryVerifyDataById(queryEventData)) {
				getSbrWriteEventEntity(event, eventEntityList, queryEventData);
			}
		}
		return eventEntityList;
	}

	/**
	 * 说明：TODO 根据当前数据库表 ID 和 创建时间 缓存判断数据是否同步
	 *
	 * @param queryEventData
	 * @return boolean
	 * @date: 2022-8-15 11:00
	 * @throws: SQLException BinlogPortalException
	 */
	private boolean queryVerifyDataById(QueryEventData queryEventData) throws SQLException, BinlogPortalException {
		DataSource source = DruidDSFactory.get(queryEventData.getDatabase());
		Entity entity = Db.use(source).queryOne(String.format("select * from %s where id=%s", StringUtils.getTableName(queryEventData.getSql()), StringUtils.getDataId(queryEventData.getSql())));

		String key = queryEventData.getDatabase().concat("-").concat(StringUtils.getTableName(queryEventData.getSql())).concat("-sbr-insert-").concat(StringUtils.getDataId(queryEventData.getSql()));
		Long createTime = Objects.isNull(entity.getDate("create_time")) ? null : entity.getDate("create_time").getTime();
		log.info("=====> Redis.update-key：{} ，Entity.create_time：[{}] <=====", key, createTime);
		Long time = positionHandler.getCacheObject(key);
		log.info("=====> Redis.update-value：{} ，Entity.create_time：[{}] <=====", time, createTime);
		if (!Objects.isNull(createTime) && !createTime.equals(time)) {
			log.info("=====> Entity.create_time：[{}] ，Redis.create_time [{}] <=====", createTime, time);
			positionHandler.setCacheObject(key, createTime, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
			return true;
		}

		log.info("=====> [SBR-INSERT] 数据创建时间字段 [create_time] 不存在！或 数据创建时间 [{}] 与 缓存创建时间 [{}] 相同、跳出同步! <=====", createTime, time);
		return false;
	}

	/**
	 * 说明：TODO 使用数据来源ID的方式
	 *
	 * @param queryEventData
	 * @return boolean
	 * @date: 2022-8-15 11:02
	 * @throws:
	 */
	private boolean queryVerifyDataByOrigin(QueryEventData queryEventData) throws SQLException, BinlogPortalException {
		Entity entity = Db.use(DruidDSFactory.get(queryEventData.getDatabase())).queryOne(String.format("select * from %s where id=%s", StringUtils.getTableName(queryEventData.getSql()), StringUtils.getDataId(queryEventData.getSql())));

		if (!Objects.isNull(entity.getStr("data_origin")) && entity.getStr("data_origin").equals(syncConfig.getDataOrigin())) {
			log.info("=====> [SBR-INSERT] 数据同步源 value：[{}] , DB 获取到数据信息：[{}] <=====", entity.getStr("data_origin"), entity.toString());
			return true;
		}

		log.info("=====> [SBR-INSERT] DB Entity.data_origin is null , Unable to sync !<=====");
		return false;
	}

	private void getSbrWriteEventEntity(Event event, List<EventEntity> eventEntityList, QueryEventData queryEventData) throws SQLException {
		DataSource source = DruidDSFactory.get(queryEventData.getDatabase());
		Entity entity = Db.use(source).queryOne(String.format("select * from %s where id=%s", StringUtils.getTableName(queryEventData.getSql()), StringUtils.getDataId(queryEventData.getSql())));
		EventEntity eventEntity = new EventEntity();
		eventEntity.setEvent(event);
		eventEntity.setDatabaseName(queryEventData.getDatabase());
		eventEntity.setTableName(StringUtils.getTableName(queryEventData.getSql()));
		eventEntity.setDataId(StringUtils.getDataId(queryEventData.getSql()));
		eventEntity.setEventEntityType(EventEntityType.INSERT);
		eventEntity.setEventEntityMode(EventEntityMode.SBR);
		eventEntity.setSql(queryEventData.getSql());
		eventEntity.setSyncIdent(Objects.isNull(entity.getDate("create_time")) ? null : entity.getDate("create_time").getTime());
		eventEntityList.add(eventEntity);

		log.debug("=====> [SBR-INSERT] 事件同步[eventEntity]实体 [{}] 个 <=====", eventEntityList.size());

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
				if (Objects.nonNull(after[i])) {
					columnData.put(columnMetaDataList.get(i).getName(), after[i]);
				}
			}

//			if (queryVerifyData(after, tableMetaEntity, columnData)) {
			if (queryVerifyDataOrigin(columnData)) {
				return;
			} else {
				columnData.put("data_origin", "02");
			}

			EventEntity eventEntity = new EventEntity();
			eventEntity.setEvent(event);
			eventEntity.setEventEntityType(EventEntityType.INSERT);
			eventEntity.setEventEntityMode(EventEntityMode.RBR);
			eventEntity.setDatabaseName(tableMetaEntity.getDbName());
			eventEntity.setTableName(tableMetaEntity.getTableName());
			eventEntity.setColumns(columns);
			eventEntity.setColumnsData(columnMetaDataList);
			eventEntity.setChangeAfter(changeAfter);
			eventEntity.setColumnData(columnData);
			eventEntity.setDataId(after[0]);
			String createTime = String.valueOf(Optional.ofNullable(columnData.get("create_time")).orElse(columnData.get("create_time")));
			eventEntity.setSyncIdent(Objects.isNull(createTime) ? null : DateUtil.parse(createTime).getTime());

			eventEntityList.add(eventEntity);
		});
		log.debug("=====> [RBR-INSERT] 事件同步[eventEntity]实体 [{}] 个 <=====", eventEntityList.size());

	}

	/**
	 * 说明：TODO 根据创建时间字段缓存 验证 是否同步
	 *
	 * @param after
	 * @param eventEntity
	 * @param columnData
	 * @return boolean
	 * @date: 2022-8-15 13:10
	 * @throws:
	 */
	private boolean queryVerifyData(String[] after, TableMetaEntity eventEntity, Map<String, Object> columnData) {
		String key = eventEntity.getDbName().concat("-").concat(eventEntity.getTableName()).concat("-rbr-insert-").concat(after[0]);
		if (Objects.isNull(columnData)) {
			log.error("====> [RBR-INSERT] 同步字段数据为空！ 跳出同步<====");
			return true;
		}

		try {
			Long time = positionHandler.getCacheObject(key);
			String date = (String) Optional.ofNullable(columnData.get("create_time")).orElse(columnData.get("create_time"));
			if (!Objects.isNull(date)) {
				Long time1 = DateUtil.parse(date).getTime();
				positionHandler.setCacheObject(key, time1, CommonConstants.TIMEOUT, TimeUnit.MINUTES);
				if (time1.equals(time)) {
					log.info("=====> [RBR-INSERT] 数据表更新时间 [update_time] 字段 [{}]  数据更新时间 [{}] 与 缓存更新时间 [{}] 相同、跳出同步! <=====", date, time1, time);
					return true;
				}
			}
		} catch (BinlogPortalException e) {
			log.error("=====> [RBR-INSERT] 验证重复数据异常信息：[{}] ，异常原因：[{}]<=====", e.getMessage(), e.getCause().getMessage());
			return true;
		}
		return false;
	}

	/**
	 * 说明：TODO RBR模式 【新增】 根据 [data_origin] 字段值 ，验证是否同步
	 *
	 * @param columnData
	 * @return boolean
	 * @date: 2022-8-15 13:11
	 * @throws:
	 */
	private boolean queryVerifyDataOrigin(Map<String, Object> columnData) {
		String origin = (String) Optional.ofNullable(columnData.get("data_origin")).orElse(columnData.get("data_origin"));
		log.info("=====> [RBR-INSERT] 数据同步源字段 [data_origin] - " + (origin == null ? "为空!" : "Value：[{}] ") + "， 同步数据：[{}]  <=====",origin,Arrays.toString(new Map[]{columnData}));
		if (!Objects.isNull(origin) && origin.equals(syncConfig.getDataOrigin())) {
			return false;
		}
		return true;
	}
}
