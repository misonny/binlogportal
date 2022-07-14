package com.insistingon.binlogportal.event;

/**
 * @author Administrator
 */

public enum EventEntityMode {
    SBR("STATEMENT模式（SBR）"),
    RBR("ROW模式（RBR）"),
    MBR("MIXED模式（MBR）");


    String desc;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    EventEntityMode(String desc) {
        this.desc = desc;
    }

}
