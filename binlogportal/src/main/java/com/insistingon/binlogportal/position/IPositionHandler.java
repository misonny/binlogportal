package com.insistingon.binlogportal.position;

import com.insistingon.binlogportal.BinlogPortalException;
import com.insistingon.binlogportal.config.SyncConfig;

import java.util.concurrent.TimeUnit;

/**
 * 处理binlog位点信息接口，实现该接口创建自定义位点处理类
 * @author Administrator
 */
public interface IPositionHandler {
    BinlogPositionEntity getPosition(SyncConfig syncConfig) throws BinlogPortalException;

    void savePosition(SyncConfig syncConfig, BinlogPositionEntity binlogPositionEntity) throws BinlogPortalException;

    /**
     * 缓存基本的对象，Integer、String、实体类等
     *
     * @param key      缓存的键值
     * @param value    缓存的值
     * @param timeout  时间
     * @param timeUnit 时间颗粒度
     */
    public <T> void setCacheObject(final String key, final T value, final Integer timeout, final TimeUnit timeUnit) throws BinlogPortalException;

    /**
     * 说明：TODO
     * @param key
     * @return T
     * @date: 2022-8-3 10:06
     */
    public <T> T getCacheObject(final String key)throws BinlogPortalException;
}
