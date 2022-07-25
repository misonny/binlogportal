package com.insistingon.binlogportal.event;

import com.github.shyiko.mysql.binlog.event.EventType;

/**
 * @author Administrator
 */
public enum EventEntityType {
	INSERT(0, "insert"),
	UPDATE(1, "update"),
	DELETE(2, "delete"),
	ALTER(3, "alter"),
	UNKNOWN(00, "未知");

	private int value;
	private String desc;

	private EventEntityType(int value, String desc) {
		this.value = value;
		this.desc = desc;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}

	EventEntityType(String desc) {
		this.desc = desc;
	}

	public static EventEntityType fromValue(Integer value) {
		EventEntityType[] var1 = values();

		for (EventEntityType s : var1) {
			if (s.value == value) {
				return s;
			}
		}
		return UNKNOWN;

	}

	public static EventEntityType fromName(String name) {
		EventEntityType[] var1 = values();

		for (EventEntityType s : var1) {
			if (s.name().equals(name)) {
				return s;
			}
		}
		return UNKNOWN;

	}

	/**
	 * 说明：TODO 是否增删改类型
	 * @param eventType
	 * @return boolean
	 * @date: 2022-7-15 11:12
	 */
	public static boolean isCrudEventType(String eventType) {

		return  eventType.equals(INSERT.name()) ||
				eventType.equals(UPDATE.name()) ||
				eventType.equals(DELETE.name());
	}

	/**
	 * 说明：TODO 修改 删除
	 * @param eventType
	 * @return boolean
	 * @date: 2022-7-15 11:16
	 */
	public static boolean isUdEventType(String eventType) {

		return  eventType.equals(UPDATE.name()) ||
				eventType.equals(DELETE.name());
	}

	/**
	 * 说明：TODO 新增
	 * @param eventType
	 * @return boolean
	 * @date: 2022-7-15 11:16
	 */
	public static boolean isInertEventType(String eventType) {

		return  eventType.equals(INSERT.name());
	}

	public static void main(String[] args) {
		System.out.println(fromName("UPDATE"));
	}
}
