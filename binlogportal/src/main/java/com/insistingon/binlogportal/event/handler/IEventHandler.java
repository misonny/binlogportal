package com.insistingon.binlogportal.event.handler;

import com.insistingon.binlogportal.event.EventEntity;
import com.insistingon.binlogportal.BinlogPortalException;

/**
 * @author Administrator
 */
public interface IEventHandler {
    public void process(EventEntity eventEntity) throws BinlogPortalException;
//    public void process(String sql) throws BinlogPortalException;
}
