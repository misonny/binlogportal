package com.insistingon.binlogportal.event;

/**
 * @author Administrator
 */

public enum EventEntityMode {
    SBR(1,"STATEMENT模式（SBR）"),
    RBR(2,"ROW模式（RBR）"),
    MBR(3,"MIXED模式（MBR）"),
    UNKNOWN(00,"未知");

    private int value;
    private String desc;
    private EventEntityMode(int value, String desc) {
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

    EventEntityMode(String desc) {
        this.desc = desc;
    }

    public static EventEntityMode fromValue(Integer value) {
        EventEntityMode[] var1 = values();

        for (EventEntityMode s : var1) {
            if (s.value == value) {
                return s;
            }
        }
        return UNKNOWN;

    }

    public static EventEntityMode fromName(String name) {
        EventEntityMode[] var1 = values();

        for (EventEntityMode s : var1) {
            if (s.name().equals(name)) {
                return s;
            }
        }
        return UNKNOWN;

    }

    public static void main(String[] args) {
        System.out.println(fromValue(1));
    }

}
