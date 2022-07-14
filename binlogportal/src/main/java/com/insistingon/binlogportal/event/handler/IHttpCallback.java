package com.insistingon.binlogportal.event.handler;

import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * @author Administrator
 */
public interface IHttpCallback {
    void call(CloseableHttpResponse closeableHttpResponse);
}
