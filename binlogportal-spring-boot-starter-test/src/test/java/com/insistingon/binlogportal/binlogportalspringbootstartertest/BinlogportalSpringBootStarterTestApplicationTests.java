package com.insistingon.binlogportal.binlogportalspringbootstartertest;

import cn.hutool.core.date.DateUtil;
import com.insistingon.binlogportal.event.EventEntityType;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BinlogportalSpringBootStarterTestApplicationTests {

	@Test
	void contextLoads() {


	}

	public static void main(String[] args) {
//		String SQL = "UPDATE `eam_dev`.`rep_demo_gongsi` SET `gname` = 'WW山海经11' WHERE `id` = 3";
//		String SQL = "UPDATE `rep_demo_gongsi` SET `gname` = 'WW山海经11' WHERE `id` = 3";
//		String SQL = "DELETE FROM `rep_demo_gongsi` WHERE `id` = 3";
		String SQL = "INSERT INTO `sys_token` (`id`, `create_time`, `is_expire`, `menu_type`, `tenant_id`, `terminal_id`, `token`, `update_time`, `user_id`, `user_name`) VALUES ('47bce1b0a9fd4d35a5ad1e34c74b6d38', timestamp('2022-07-15 09:37:17.835449'), false, 15, 120010163938791424, 1464077791413800960, 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIxNDI4MjkyMDk2MDA3MjEzMDU2IiwidGlkIjoiMTIwMDEwMTYzOTM4NzkxNDI0IiwibXR5IjoiMTUiLCJkaWQiOiIxNDY0MDc3NzkxNDEzODAwOTYwIiwiaWF0IjoiMTY1Nzg3NzgzNyIsIm5iZiI6MTY1Nzg0OTAzNywiZXhwIjoxNjU3OTM1NDM3LCJpc3MiOiJib2FyZG1lZCIsImF1ZCI6ImJvYXJkbWVkIn0.CNoFCTZtU1j4JWo05WzBudB5FJQj2clzbF-A3DL8aEA', timestamp('2022-07-15 09:37:17.835455'), 1428292096007213056, '周露玲')";
//		String SQL = "INSERT INTO `eam_dev`.`sys_login_log` (`id`, `create_id`, `create_name`, `create_time`, `is_delete`, `login_fail_msg`, `login_ip`, `login_result`, `login_system`, `login_time`, `tenant_id`, `update_id`, `update_name`, `update_time`, `user_account`, `user_agent`, `user_id`, `user_name`) VALUES (1547757298057875456, 1428292096007213056, '周露玲', timestamp('2022-07-15 09:37:50.454915'), false, NULL, '192.168.5.77', true, '试剂中心库PDA', timestamp('2022-07-15 09:37:50.454900'), 120010163938791424, 1428292096007213056, '周露玲', timestamp('2022-07-15 09:37:50.454919'), 'zhoululing', NULL, 1428292096007213056, '周露玲')";
//		getTableName(SQL);
//		getDataId(SQL);
		System.out.println(DateUtil.dateSecond().getTime()/1000);
		System.out.println(DateUtil.dateSecond().getTime());
		System.out.println(DateUtil.parse("2021-07-11 13:38:51").getTime());

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

}