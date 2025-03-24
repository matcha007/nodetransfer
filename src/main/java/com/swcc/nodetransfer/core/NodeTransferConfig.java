package com.swcc.nodetransfer.core;

import com.swcc.nodetransfer.core.route.ShortestPathRouteServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;

public class NodeTransferConfig {
    private final String currentNodeCode;
    private NettyClientConfig nettyClientConfig;
    private NettyServerConfig nettyServerConfig;
    private RouteServer routeServer;
    private long forwardTimeoutMillis = 5000L;

    public NodeTransferConfig(String currentNodeCode) {
        this(currentNodeCode, new ShortestPathRouteServer(), 63301);
    }

    public NodeTransferConfig(String currentNodeCode, RouteServer routeServer, int listenPort) {
        this.currentNodeCode = currentNodeCode;
        this.routeServer = routeServer;

        nettyClientConfig = new NettyClientConfig();
        nettyClientConfig.setClientOnewaySemaphoreValue(10000);
        nettyClientConfig.setClientAsyncSemaphoreValue(10000);

        nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(listenPort);
    }

    public String getCurrentNodeCode() {
        return currentNodeCode;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public void setNettyClientConfig(NettyClientConfig nettyClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public void setNettyServerConfig(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
    }

    public RouteServer getRouteServer() {
        return routeServer;
    }

    public void setRouteServer(RouteServer routeServer) {
        this.routeServer = routeServer;
    }

    public long getForwardTimeoutMillis() {
        return forwardTimeoutMillis;
    }

    public void setForwardTimeoutMillis(long forwardTimeoutMillis) {
        this.forwardTimeoutMillis = forwardTimeoutMillis;
    }
}
