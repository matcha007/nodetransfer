package com.duansx.nodetransfer.client.dispatcher;

public interface Dispatcher {

    String name();

    void pushMessage(PushRequest pushRequest);

    void updateTargets();

    void doDispatch();
}
