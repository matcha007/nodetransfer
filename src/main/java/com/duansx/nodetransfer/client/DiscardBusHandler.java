package com.duansx.nodetransfer.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscardBusHandler implements BusHandler<String> {
    private static final Logger logger = LoggerFactory.getLogger(DiscardBusHandler.class);
    @Override
    public Object receive(String busObject) throws Exception {
        logger.info(busObject);
        return "DiscardBusHandler";
    }
}
