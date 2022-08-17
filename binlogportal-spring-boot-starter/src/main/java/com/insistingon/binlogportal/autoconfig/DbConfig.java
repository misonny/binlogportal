package com.insistingon.binlogportal.autoconfig;

import java.util.List;

/**
 * @author Administrator
 */
public class DbConfig {
    String host;
    Integer port;
    String userName;
    String password;
    List<String> handlerList;
    List<String> databaseName;
    List<String> dataTables;
    String dataOrigin;



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

    public List<String> getHandlerList() {
        return handlerList;
    }

    public void setHandlerList(List<String> handlerList) {
        this.handlerList = handlerList;
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

    public String getDataOrigin() {
        return dataOrigin;
    }

    public void setDataOrigin(String dataOrigin) {
        this.dataOrigin = dataOrigin;
    }
}
