package com.duansx.nodetransfer.core;

public interface RouteServer {

    String findNextNodeAddr(String sourceNodeCode, String targetNodeCode) throws RouteNotFoundException;

}
