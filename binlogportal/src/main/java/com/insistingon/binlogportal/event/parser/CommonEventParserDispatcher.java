package com.insistingon.binlogportal.event.parser;

import com.alibaba.druid.support.json.JSONUtils;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.insistingon.binlogportal.BinlogPortalException;
import com.insistingon.binlogportal.config.SyncConfig;
import com.insistingon.binlogportal.event.EventEntity;
import com.insistingon.binlogportal.event.EventEntityType;
import com.insistingon.binlogportal.position.IPositionHandler;
import com.insistingon.binlogportal.tablemeta.TableMetaFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通用事件解析调度器
 * 根据不同事件类型调用事件解析器
 *
 * @author Administrator
 */
public class CommonEventParserDispatcher implements IEventParserDispatcher {
	private final Logger log = LoggerFactory.getLogger(CommonEventParserDispatcher.class);
	private static final Map<Long, String> TABLE_MAP = new HashMap<>();
	/**
	 * 数据表元数据工厂
	 */
	TableMetaFactory tableMetaFactory;
	/**
	 * 同步信息配置
	 */
	SyncConfig syncConfig;

	public TableMetaFactory getTableMetaFactory() {
		return tableMetaFactory;
	}

	public void setTableMetaFactory(TableMetaFactory tableMetaFactory) {
		this.tableMetaFactory = tableMetaFactory;
	}

	public SyncConfig getSyncConfig() {
		return syncConfig;
	}

	public void setSyncConfig(SyncConfig syncConfig) {
		this.syncConfig = syncConfig;
	}

	public CommonEventParserDispatcher(TableMetaFactory tableMetaFactory, IPositionHandler positionHandler) {
		this.tableMetaFactory = tableMetaFactory;
		this.syncConfig = tableMetaFactory.getSyncConfig();
		this.updateEventParser = new UpdateEventParser(tableMetaFactory,positionHandler);
		this.insertEventParser = new InsertEventParser(tableMetaFactory,positionHandler);
		this.deleteEventParser = new DeleteEventParser(tableMetaFactory,positionHandler);
	}

	/**
	 * 更新事件解析器
	 */
	IEventParser updateEventParser;

	/**
	 * 插入事件解析器
	 */
	IEventParser insertEventParser;

	/**
	 * 删除事件解析器
	 */
	IEventParser deleteEventParser;

	@Override
	public List<EventEntity> parse(Event event) throws BinlogPortalException, SQLException, NoSuchFieldException {
		/*
		 * table_id不固定对应一个表，它是表载入table cache时临时分配的，一个不断增长的变量
		 * 连续往同一个table中进行多次DML操作，table_id不变。 一般来说，出现DDL操作时，table_id才会变化
		 * 所有更新和插入操作，都会产生一个TABLE_MAP事件
		 * 通过该事件缓存table_id对应的表信息，然后再处理对应的事件
		 */
		tableMetaFactory(event);

//		log.debug("=====> 指定同步数据库名称：{} ，数据库表：{} <=====", syncConfig.getDatabaseName().toString(), syncConfig.getDataTables().toString());

		/**
		 * 如已配置 指定数据库名 或 数据库表 则根据配置表信息同步
		 */
//		if (com.insistingon.binlogportal.event.parser.utils.StringUtils.isDbOrTableNull(syncConfig, tableMetaEntity)) {
//			return getEventEntityList(event);
//		}
		/**
		 * 如未配置指定库和表名则所有同步
		 */
//		if (com.insistingon.binlogportal.event.parser.utils.StringUtils.isDbAndTableNull(syncConfig)){
//			return getEventEntityList(event);
//		}
		return getEventEntityList(event);
	}

	private List<EventEntity> getEventEntityList(Event event) throws BinlogPortalException, SQLException {
		if (EventType.QUERY.equals(event.getHeader().getEventType())) {
//			log.debug("======> 进入[SBR]事件信息处理：{} <====== ", event.getData().toString());
			QueryEventData queryEventData = event.getData();
			/**
			 * (queryEventData.getSql().contains(EventEntityType.INSERT.name()||queryEventData.getSql().contains(EventEntityType.ALTER.name())
			 *  根据类型判断是否更新表结构
			 */
			if (!StringUtils.isBlank(queryEventData.getSql()) && queryEventData.getSql().contains(EventEntityType.INSERT.name())) {
//				log.debug("=====> 进入[SBR] 新增事件，{}", queryEventData.getSql());
				return insertEventParser.parse(event);
			}

			if (!StringUtils.isBlank(queryEventData.getSql()) && queryEventData.getSql().contains(EventEntityType.UPDATE.name())) {
//				log.debug("=====>  进入[SBR] 更新事件，{}", queryEventData.getSql());
				return updateEventParser.parse(event);
			}

			if (!StringUtils.isBlank(queryEventData.getSql()) && queryEventData.getSql().contains(EventEntityType.DELETE.name())) {
//				log.debug("=====>  进入[SBR] 删除事件，{}", queryEventData.getSql());
				return deleteEventParser.parse(event);
			}


		}

		//处理更新事件
		if (EventType.isUpdate(event.getHeader().getEventType())) {
//			log.debug("=====> 进入[RBR] 更新处理事件，{}", event.getData().toString());
			return updateEventParser.parse(event);
		}

		//处理插入事件
		if (EventType.isWrite(event.getHeader().getEventType())) {
//			log.debug("=====> 进入[RBR] 新增处理事件，{}",event.getData().toString());
			return insertEventParser.parse(event);
		}

		//删除事件处理
		if (EventType.isDelete(event.getHeader().getEventType())) {
//			log.debug("=====> 执行[RBR] 删除处理事件，{}", event.getData().toString());
			return deleteEventParser.parse(event);
		}


		return null;
	}

	private void tableMetaFactory(Event event) throws BinlogPortalException, NoSuchFieldException {

		if (EventType.TABLE_MAP.equals(event.getHeader().getEventType())) {
			TableMapEventData tableMapEventData = event.getData();

			if (!syncConfig.getDatabaseName().contains(tableMapEventData.getDatabase())){
				log.debug("=====> 未配置同步数据库：[{}] 、跳过缓存<=====",tableMapEventData.getDatabase());
				return;
			}
				log.debug("=====> 已配置同步数据库进行缓存信息：[{}] <=====",tableMapEventData.toString());
			//table_map事件，要更新下tableMetaFactory中的tableId对应的信息缓存
			tableMetaFactory.getTableMetaEntity(
					tableMapEventData.getTableId(),
					tableMapEventData.getDatabase(),
					tableMapEventData.getTable()
			);

		}

	}
}
