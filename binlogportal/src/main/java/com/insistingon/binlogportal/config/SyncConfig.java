package com.insistingon.binlogportal.config;


import com.insistingon.binlogportal.event.handler.IEventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Administrator
 */
public class SyncConfig {
	String host;
	Integer port;
	String userName;
	String password;
	List<IEventHandler> eventHandlerList = new ArrayList<>();
	List<String> databaseName = new ArrayList<>();
	List<String> dataTables = new ArrayList<>();

	public SyncConfig() {
	}

	public SyncConfig(String host, Integer port, String userName, String password) {
		this.host = host;
		this.port = port;
		this.userName = userName;
		this.password = password;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public List<IEventHandler> getEventHandlerList() {
		return eventHandlerList;
	}

	public void setEventHandlerList(List<IEventHandler> eventHandlerList) {
		this.eventHandlerList = eventHandlerList;
	}

	public void addEventHandlerList(IEventHandler eventHandler) {
		eventHandlerList.add(eventHandler);
	}

	public List<String> getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(List<String> databaseName) {
		this.databaseName = databaseName;
	}

	public List<String> getDataTables() {
		return dataTables;
	}

	public void setDataTables(List<String> dataTables) {
		this.dataTables = dataTables;
	}

	@Override
	public String toString() {
		return "SyncConfig{" +
				"host='" + host + '\'' +
				", port=" + port +
				", userName='" + userName + '\'' +
				", password='" + password + '\'' +
				", eventHandlerList=" + eventHandlerList +
				", databaseName=" + databaseName +
				", dataTables=" + dataTables +
				'}';
	}
}
