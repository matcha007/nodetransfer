package com.swcc.nodetransfer.core;

public class RouteNotFoundException extends Exception {
    public RouteNotFoundException(String curNodeCode, String targetNodeCode) {
        super(String.format("route of [%s]-->[%s] is blank", curNodeCode, targetNodeCode));
    }
}
