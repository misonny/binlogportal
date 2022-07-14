package com.insistingon.binlogportal.event;

/**
 * @author Administrator
 */

public enum EventEntityType {
    UPDATE("update"),
    INSERT("insert"),
    DELETE("delete"),
    ALTER("alter");

    String desc;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    EventEntityType(String desc) {
        this.desc = desc;
    }

}
