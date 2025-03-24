package com.duansx.nodetransfer.client;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscardRemotingProcessor implements NettyRequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(DiscardRemotingProcessor.class);

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand request) throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(0, null);
        response.setOpaque(request.getOpaque());
        logger.info(request.toString());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
