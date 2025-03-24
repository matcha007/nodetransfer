package com.swcc.nodetransfer.client.dispatcher;


import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class PushMessageService extends ServiceThread {
    private static final Logger logger = LoggerFactory.getLogger(PushMessageService.class);
    private final LinkedBlockingQueue<PushRequest> pushRequestQueue = new LinkedBlockingQueue<>();
    private final DispatcherFactory dispatcherFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "PushMessageServiceScheduledThread");
        }
    });

    public PushMessageService(DispatcherFactory dispatcherFactory) {
        this.dispatcherFactory = dispatcherFactory;
    }

    public void executePushRequestLater(PushRequest pushRequest, long timeDelay) {
        if (!this.isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PushMessageService.this.executePushRequestImmediately(pushRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PushMessageServiceScheduledThread has shutdown");
        }

    }

    public void executePushRequestImmediately(PushRequest pushRequest) {
        try {
            this.pushRequestQueue.put(pushRequest);
        } catch (InterruptedException var3) {
            logger.error("executePushRequestImmediately pushRequestQueue.put", var3);
        }

    }

    public void executeTaskLater(Runnable r, long timeDelay) {
        if (!this.isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            logger.warn("PushMessageServiceScheduledThread has shutdown");
        }

    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return this.scheduledExecutorService;
    }

    private void pushMessage(PushRequest pushRequest) {
        Dispatcher dispatcher = this.dispatcherFactory.selectDispatcher(pushRequest.getName());
        if (dispatcher != null) {
            dispatcher.pushMessage(pushRequest);
        } else {
            logger.warn("No matched dispatcher for the PushRequest {}, drop it", pushRequest);
        }
    }

    @Override
    public void run() {
        logger.info(this.getServiceName() + " service started");

        while(!this.isStopped()) {
            try {
                PushRequest pushRequest = this.pushRequestQueue.take();
                this.pushMessage(pushRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                logger.error("Push Message Service Run Method exception", e);
            }
        }

        logger.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000L, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PushMessageService.class.getSimpleName();
    }

}
