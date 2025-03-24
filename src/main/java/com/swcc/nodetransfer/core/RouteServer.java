package com.swcc.nodetransfer.core;

public interface RouteServer {

    String findNextNodeAddr(String sourceNodeCode, String targetNodeCode) throws RouteNotFoundException;

}
