package com.swcc.nodetransfer.client.dispatcher;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

public class DispatchService extends ServiceThread {
    private static final long waitInterval = Long.parseLong(System.getProperty("nodetransfer.client.dispatch.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final DispatcherFactory dispatcherFactory;

    public DispatchService(DispatcherFactory dispatcherFactory) {
        this.dispatcherFactory = dispatcherFactory;
    }

    @Override
    public void run() {
        this.log.info(this.getServiceName() + " service started");

        while(!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.dispatcherFactory.doDispatch();
        }

        this.log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return DispatchService.class.getSimpleName();
    }
}
