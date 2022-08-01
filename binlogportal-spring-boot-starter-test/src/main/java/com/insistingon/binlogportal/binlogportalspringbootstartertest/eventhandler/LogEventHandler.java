package com.insistingon.binlogportal.binlogportalspringbootstartertest.eventhandler;

import com.insistingon.binlogportal.BinlogPortalException;
import com.insistingon.binlogportal.event.EventEntity;
import com.insistingon.binlogportal.event.handler.IEventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author Administrator
 */
@Component
@Slf4j
public class LogEventHandler implements IEventHandler {
    @Override
    public void process(EventEntity eventEntity) throws BinlogPortalException {
         log.info("======> 进入日志监听事件，参数信息：{} <======",eventEntity.getJsonFormatData());
    }

//    @Override
//    public void process(String sql) throws BinlogPortalException {
//
//    }
}
