package com.swcc.nodetransfer.client;

public interface NodeTransferCallBack<T> {
    void onSuccess(T object);

    void onException(Throwable throwable);
}
