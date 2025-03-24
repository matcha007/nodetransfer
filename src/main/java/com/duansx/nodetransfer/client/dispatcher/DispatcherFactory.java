package com.duansx.nodetransfer.client.dispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

public class DispatcherFactory {

    private static final Logger logger = LoggerFactory.getLogger(DispatcherFactory.class);

    private final ConcurrentMap<String, Dispatcher> dispatcherTable;
    private final ScheduledExecutorService scheduledExecutorService;
    private final PushMessageService pushMessageService;
    private final DispatchService dispatchService;

    public DispatcherFactory() {
        this.dispatcherTable = new ConcurrentHashMap<>();

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "DispatcherFactoryScheduledThread");
            }
        });

        this.pushMessageService = new PushMessageService(this);
        this.dispatchService = new DispatchService(this);
    }

    public void start() {
        this.startScheduledTask();
        pushMessageService.start();
        dispatchService.start();
    }

    public void shutdown() {
        pushMessageService.shutdown();
        dispatchService.shutdown();
    }

    private void startScheduledTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    updateTargets();
                } catch (Exception e) {
                    logger.error("ScheduledTask updateTargets exception", e);
                }

            }
        }, 10L, 3000L, TimeUnit.MILLISECONDS);
    }

    public void updateTargets() {
        for (Map.Entry<String, Dispatcher> entry : this.dispatcherTable.entrySet()) {
            Dispatcher dispatcher = entry.getValue();
            if (dispatcher != null) {
                try {
                    dispatcher.updateTargets();
                } catch (Throwable throwable) {
                    logger.error("updateTargets exception", throwable);
                }
            }
        }
    }

    public void doDispatch() {
        for (Map.Entry<String, Dispatcher> entry : this.dispatcherTable.entrySet()) {
            Dispatcher dispatcher = entry.getValue();
            if (dispatcher != null) {
                try {
                    dispatcher.doDispatch();
                } catch (Throwable throwable) {
                    logger.error("doDispatch exception", throwable);
                }
            }
        }
    }


    public boolean registerDispatcher(String name, Dispatcher dispatcher) {
        if (null != name && null != dispatcher) {
            Dispatcher prev = this.dispatcherTable.putIfAbsent(name, dispatcher);
            if (prev != null) {
                logger.warn("the dispatcher group[{}] exist already.", name);
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    public void unregisterDispatcher(String name) {
        this.dispatcherTable.remove(name);
    }

    public Dispatcher selectDispatcher(String name) {
        return dispatcherTable.get(name);
    }


    public PushMessageService getPushMessageService() {
        return pushMessageService;
    }
}
