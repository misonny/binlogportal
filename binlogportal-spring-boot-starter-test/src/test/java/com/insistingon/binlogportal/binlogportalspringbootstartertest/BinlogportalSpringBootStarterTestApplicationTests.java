package com.insistingon.binlogportal.binlogportalspringbootstartertest;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BinlogportalSpringBootStarterTestApplicationTests {

	@Test
	void contextLoads() {


	}

	public static void main(String[] args) {
		String SQL = "UPDATE `demo`.`order_analysis` SET `cancel_order` = 1191 WHERE `id` = 1000";
		SQL = SQL.substring(SQL.indexOf(".") + 2);
		SQL = SQL.substring(0, SQL.indexOf("`"));
		System.out.println(SQL);
	}
}
