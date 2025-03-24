package com.swcc.nodetransfer.client.dispatcher;

import java.util.StringJoiner;

public class PushRequest {

    private static final long PUSH_MAX_IDLE_TIME = Long.parseLong(System.getProperty("nodetransfer.client.push.pushMaxIdleTime", "120000"));;

    private final String name;

    private final String target;

    private volatile boolean dropped = false;

    private volatile long lastPushTimestamp = System.currentTimeMillis();

    public PushRequest(String name, String target) {
        this.name = name;
        this.target = target;
    }

    public boolean isPullExpired() {
        return System.currentTimeMillis() - this.lastPushTimestamp > PUSH_MAX_IDLE_TIME;
    }

    public String getName() {
        return name;
    }

    public String getTarget() {
        return target;
    }

    public boolean isDropped() {
        return this.dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public long getLastPushTimestamp() {
        return lastPushTimestamp;
    }

    public void setLastPushTimestamp(long lastPushTimestamp) {
        this.lastPushTimestamp = lastPushTimestamp;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PushRequest.class.getSimpleName() + "[", "]").add("name='" + name + "'").add("target='" + target + "'").add("dropped=" + dropped).toString();
    }
}
