package com.swcc.nodetransfer.client;

public interface BusHandler<T> {

    Object receive(T busObject) throws Exception;

}
