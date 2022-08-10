package com.insistingon.binlogportal.binlogportalspringbootstartertest;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.db.sql.SqlUtil;
import com.insistingon.binlogportal.event.EventEntityType;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;

@SpringBootTest
class BinlogportalSpringBootStarterTestApplicationTests {

	@Test
	void contextLoads() {


	}

	public static void main(String[] args) {
//		String SQL = "UPDATE `eam_dev`.`rep_demo_gongsi` SET `gname` = 'WW山海经11' WHERE `id` = 3";
//		String SQL = "UPDATE `rep_demo_gongsi` SET `gname` = 'WW山海经11' WHERE `id` = 3";
//		String SQL = "DELETE FROM `rep_demo_gongsi` WHERE `id` = 3";
//		String SQL = "INSERT INTO `sys_token` (`id`, `create_time`, `is_expire`, `menu_type`, `tenant_id`, `terminal_id`, `token`, `update_time`, `user_id`, `user_name`) VALUES ('47bce1b0a9fd4d35a5ad1e34c74b6d38', timestamp('2022-07-15 09:37:17.835449'), false, 15, 120010163938791424, 1464077791413800960, 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIxNDI4MjkyMDk2MDA3MjEzMDU2IiwidGlkIjoiMTIwMDEwMTYzOTM4NzkxNDI0IiwibXR5IjoiMTUiLCJkaWQiOiIxNDY0MDc3NzkxNDEzODAwOTYwIiwiaWF0IjoiMTY1Nzg3NzgzNyIsIm5iZiI6MTY1Nzg0OTAzNywiZXhwIjoxNjU3OTM1NDM3LCJpc3MiOiJib2FyZG1lZCIsImF1ZCI6ImJvYXJkbWVkIn0.CNoFCTZtU1j4JWo05WzBudB5FJQj2clzbF-A3DL8aEA', timestamp('2022-07-15 09:37:17.835455'), 1428292096007213056, '周露玲')";
//		String SQL = "INSERT INTO `eam_dev`.`sys_login_log` (`id`, `create_id`, `create_name`, `create_time`, `is_delete`, `login_fail_msg`, `login_ip`, `login_result`, `login_system`, `login_time`, `tenant_id`, `update_id`, `update_name`, `update_time`, `user_account`, `user_agent`, `user_id`, `user_name`) VALUES (1547757298057875456, 1428292096007213056, '周露玲', timestamp('2022-07-15 09:37:50.454915'), false, NULL, '192.168.5.77', true, '试剂中心库PDA', timestamp('2022-07-15 09:37:50.454900'), 120010163938791424, 1428292096007213056, '周露玲', timestamp('2022-07-15 09:37:50.454919'), 'zhoululing', NULL, 1428292096007213056, '周露玲')";
		String SQL = "INSERT INTO `hos_depot_stock_department_record` (`id`, `after_quantity`, `before_quantity`, `create_id`, `create_name`, `create_time`, `data_origin`, `department_id`, `depot_id`, `is_delete`, `is_outbound`, `manufacturer_id`, `manufacturer_name`, `operate_id`, `operate_name`, `operate_time`, `product_code`, `product_id`, `product_list_id`, `product_model`, `product_name`, `product_short_name`, `product_short_spec`, `product_spec`, `product_unit`, `quantity`, `remarks`, `supplier_id`, `supplier_name`, `total_money`, `type`, `update_id`, `update_name`, `update_time`) VALUES (1554719228140064768, 26, 0, 1534104927150936064, '王华', timestamp('2022-08-03 14:42:03.932526'), '01', 1475723190155218944, 0, false, false, 0, NULL, 1534104927150936064, '王华', timestamp('2022-08-03 14:42:03.925863'), NULL, 1546790727193858048, 1546790727227412480, NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, 1540141578415378432, '和贯科技', 100.0000, 1, 1534104927150936064, '王华', timestamp('2022-08-03 14:42:03.932564'))";
//		getTableName(SQL);
//		getDataId(SQL);

//		System.out.println(DateUtil.dateSecond().getTime()/1000);
//		System.out.println(DateUtil.dateSecond().getTime() / 1000);
//		System.out.println(DateUtil.parse("2021-07-11 13:38:52").getTime() / 1000);
//		System.out.println(DateUtil.parse("2021-07-11 13:38:53").getTime());

		getColumnValue(SQL,"create_time");

	}

	public static Long getTimes(String date){
		DateUtil.dateSecond();
		return null;
	}

	public static String getTableName(String SQL) {
		String EventType = SQL.substring(0, 6);
		if (!StringUtils.isBlank(SQL) && !SQL.contains("`.`")) {
			if (EventEntityType.isCrudEventType(EventType)) {
				System.out.println("语句类型：" + EventType);
				SQL = SQL.substring(SQL.indexOf("`") + 1);
				SQL = StringUtils.substringBefore(SQL, "`");
				System.out.println("不包含数据库名称：" + SQL);
				return SQL;
			}

		}
		if (!StringUtils.isBlank(SQL) && SQL.contains("`.`")) {
			SQL = SQL.substring(SQL.indexOf("`.`") + 3);
			SQL = SQL.substring(0, SQL.indexOf("`"));
			System.out.println("包含数据库名称：" + SQL);
			return SQL;
		}
		return SQL;
	}

	public static String getDataId(String SQL) {
		if (!StringUtils.isBlank(SQL)) {
			String EventType = SQL.substring(0, 6);
			System.out.println("执行SQL语句类型：" + EventType);
			if (EventEntityType.isUdEventType(EventType)) {
				SQL = StringUtils.substringAfterLast(SQL,"=").trim();
				System.out.println("数据ID：" + SQL);
				if (SQL.contains("'")) {
					SQL = StringUtils.substring(SQL, 1, SQL.lastIndexOf("'"));
					System.out.println("去除单引号数据ID：" + SQL);
				}
			}

			if (EventEntityType.isInertEventType(EventType)) {
				SQL = StringUtils.substringAfter(SQL, "VALUES");
				SQL = StringUtils.substring(SQL, SQL.indexOf("(") + 1, SQL.indexOf(","));
				if (SQL.contains("'")) {
					SQL = StringUtils.substring(SQL, 1, SQL.lastIndexOf("'"));
					System.out.println("数据ID：" + SQL);
				}
				System.out.println("数据ID：" + SQL);
				return SQL;
			}
			return SQL;
		}

		return SQL;
	}

	public static  String getColumnValue(String SQL,String column){
		if (!StringUtils.isBlank(SQL)) {
			String EventType = SQL.substring(0, 6);

			/**
			 * 修改、删除语句
			 */
			if (EventEntityType.isUdEventType(EventType)) {
				String[] strings = StringUtils.substringsBetween(SQL, "(", ")");
				System.out.println("字符集：" + strings);
				if (SQL.contains("'")) {
					SQL = StringUtils.substring(SQL, 1, SQL.lastIndexOf("'"));
					System.out.println("去除单引号数据ID：" + SQL);
				}
				return SQL;
			}

			/**
			 * 新增语句
			 */
			if (EventEntityType.isInertEventType(EventType)) {
				String strings = StrUtil.subBetween(SQL, "(", ")");
				System.out.println("数据字段：" + strings);
				SQL = StringUtils.substringAfter(SQL, "VALUES");
				SQL = StrUtil.subBetween(SQL, "timestamp(", ")");
				System.out.println("数据值：" + SQL);
				return SQL;
			}
		}
		return null;
	}

}