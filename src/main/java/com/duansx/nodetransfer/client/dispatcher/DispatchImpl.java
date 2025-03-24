package com.duansx.nodetransfer.client.dispatcher;

import cn.hutool.core.collection.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DispatchImpl {

    private static final Logger logger = LoggerFactory.getLogger(DispatchImpl.class);

    protected final ConcurrentMap<String, PushRequest> pushRequestTable = new ConcurrentHashMap<>();
    protected final ConcurrentHashSet<String> targets = new ConcurrentHashSet<>();
    private final Dispatcher dispatcher;

    public DispatchImpl(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void doDispatch() {
        Iterator<Map.Entry<String, PushRequest>> pushRequestIterator = pushRequestTable.entrySet().iterator();
        while (pushRequestIterator.hasNext()) {
            Map.Entry<String, PushRequest> next = pushRequestIterator.next();
            PushRequest pushRequest = next.getValue();
            String target = pushRequest.getTarget();
            if (!targets.contains(target)) {
                pushRequest.setDropped(true);
                pushRequestIterator.remove();
                logger.info("doDispatch, {}, remove unnecessary pushRequest, {}", dispatcher.name(), pushRequest);
            } else if (pushRequest.isPullExpired()) {
                pushRequest.setDropped(true);
                pushRequestIterator.remove();
                logger.error("[BUG]doDispatch, {}, remove unnecessary pushRequest, {}, because push is pause, so try to fixed it", dispatcher.name(), pushRequest);
            }
        }


        List<PushRequest> pushRequests = new ArrayList<>();
        for (String target : targets) {
            PushRequest pushRequest = new PushRequest(dispatcher.name(), target);
            if (pushRequestTable.putIfAbsent(target, pushRequest) != null) {
                logger.info("doDispatch, {}, pushRequest already exists, {}", dispatcher.name(), pushRequest);
            }else {
                logger.info("doDispatch, {}, add a new pushRequest, {}", dispatcher.name(), pushRequest);
                pushRequests.add(pushRequest);
            }
        }

        for (PushRequest pushRequest : pushRequests) {
            dispatcher.pushMessage(pushRequest);
        }
    }

    public ConcurrentMap<String, PushRequest> getPushRequestTable() {
        return pushRequestTable;
    }

    public ConcurrentHashSet<String> getTargets() {
        return targets;
    }
}
