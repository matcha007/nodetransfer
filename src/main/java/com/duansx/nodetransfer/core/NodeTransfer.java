package com.duansx.nodetransfer.core;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.*;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class NodeTransfer {

    private static final Logger logger = LoggerFactory.getLogger(NodeTransfer.class);
    public static final String SOURCE_NODE_CODE = "_SOURCE_NODE_CODE_";
    public static final String TARGET_NODE_CODE = "_TARGET_NODE_CODE_";
    public static final String FLAG = "_FLAG_";
    private String currentNodeCode;
    private Sender sender;
    private Receiver receiver;
    private RouteServer routeServer;
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;
    private long forwardTimeoutMillis;
    private final ConcurrentMap<String, String> nextNodeAddrTables;

    public NodeTransfer(String currentNodeCode) {
        this(new NodeTransferConfig(currentNodeCode));
    }

    public NodeTransfer(NodeTransferConfig nodeTransferConfig) {
        this.currentNodeCode = nodeTransferConfig.getCurrentNodeCode();
        this.routeServer = nodeTransferConfig.getRouteServer();
        this.sender = new Sender(nodeTransferConfig.getNettyClientConfig());
        this.receiver = new Receiver(nodeTransferConfig.getNettyServerConfig());
        this.nextNodeAddrTables = new ConcurrentHashMap<>();
        this.forwardTimeoutMillis = nodeTransferConfig.getForwardTimeoutMillis();
    }

    public void start() {
        sender.start();
        receiver.start();
    }

    public void shutdown() {
        sender.shutdown();
        receiver.shutdown();
    }

    public RemotingCommand invokeSync(String targetNodeCode, RemotingCommand request, long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RouteNotFoundException {
        String nextNodeAddr = getNextNodeAddr(targetNodeCode);
        addTransferFields(request, targetNodeCode);
        return sender.invokeSync(nextNodeAddr, request, timeoutMillis);
    }

    public void invokeAsync(String targetNodeCode, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, RouteNotFoundException {
        String nextNodeAddr = getNextNodeAddr(targetNodeCode);
        addTransferFields(request, targetNodeCode);
        sender.invokeAsync(nextNodeAddr, request, timeoutMillis, invokeCallback);
    }

    public void invokeOneway(String targetNodeCode, RemotingCommand request, long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, RouteNotFoundException {
        String nextNodeAddr = getNextNodeAddr(targetNodeCode);
        addTransferFields(request, targetNodeCode);
        sender.invokeOneway(nextNodeAddr, request, timeoutMillis);
    }

    private void addTransferFields(RemotingCommand request, String targetNodeCode) {
        request.addExtField(SOURCE_NODE_CODE, currentNodeCode);
        request.addExtField(TARGET_NODE_CODE, targetNodeCode);
    }

    private void addTransferFields(RemotingCommand response, RemotingCommand msg) {
        HashMap<String, String> extFields = msg.getExtFields();
        String sourceNodeCode = extFields.get(SOURCE_NODE_CODE);
        response.addExtField(SOURCE_NODE_CODE, currentNodeCode);
        response.addExtField(TARGET_NODE_CODE, sourceNodeCode);
    }

    public void setCurrentNodeCode(String currentNodeCode) {
        this.currentNodeCode = currentNodeCode;
    }

    public String getCurrentNodeCode() {
        return currentNodeCode;
    }

    public Sender getSender() {
        return sender;
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public void setReceiver(Receiver receiver) {
        this.receiver = receiver;
    }

    public RouteServer getRouteServer() {
        return routeServer;
    }

    public void setRouteServer(RouteServer routeServer) {
        this.routeServer = routeServer;
    }

    public long getForwardTimeoutMillis() {
        return forwardTimeoutMillis;
    }

    public void setForwardTimeoutMillis(long forwardTimeoutMillis) {
        this.forwardTimeoutMillis = forwardTimeoutMillis;
    }

    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        receiver.registerProcessor(requestCode, processor, executor);
    }

    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair<>(processor, executor);
    }

    public String getNextNodeAddr(String targetNodeCode) throws RouteNotFoundException {
        String nextNodeAddr = nextNodeAddrTables.get(targetNodeCode);
        if (nextNodeAddr == null) {
            nextNodeAddr = routeServer.findNextNodeAddr(currentNodeCode, targetNodeCode);
            nextNodeAddrTables.put(targetNodeCode, nextNodeAddr);
            return nextNodeAddr;
        }
        return nextNodeAddr;
    }

    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            HashMap<String, String> extFields = msg.getExtFields();
            String targetNodeCode = extFields.get(TARGET_NODE_CODE);
            String flag = extFields.get(FLAG);
            if (flag != null) {
                msg.setFlag(Integer.parseInt(extFields.get(FLAG)));
            } else {
                msg.addExtField(FLAG, msg.getFlag() + "");
            }

            if (!currentNodeCode.equals(targetNodeCode)) {
                this.forward(ctx, msg, targetNodeCode);
            } else {
                switch (msg.getType()) {
                    case REQUEST_COMMAND:
                        this.processRequestCommand(ctx, msg);
                        break;
                    case RESPONSE_COMMAND:
                        this.processResponseCommand(ctx, msg);
                }
            }
        }
    }

    private void forward(ChannelHandlerContext ctx, RemotingCommand msg, String targetNodeCode) {
        try {
            String nextNodeAddr = getNextNodeAddr(targetNodeCode);
            sender.invokeOneway(nextNodeAddr, msg, forwardTimeoutMillis);
        } catch (Exception e) {
            logger.error("process forward exception", e);
            logger.error(msg.toString());
            if (!msg.isOnewayRPC()) {
                RemotingCommand responsex = RemotingCommand.createResponseCommand(1, RemotingHelper.exceptionSimpleDesc(e));
                responsex.setOpaque(msg.getOpaque());
                addTransferFields(responsex, msg);
                ctx.writeAndFlush(responsex);
            }
        }
    }

    public void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        Pair<NettyRequestProcessor, ExecutorService> matched = receiver.getProcessorPair(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? defaultRequestProcessor : matched;
        final int opaque = cmd.getOpaque();
        RemotingCommand response;
        if (pair != null) {
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        RemotingCommand response = pair.getObject1().processRequest(ctx, cmd);
                        doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                        if (!cmd.isOnewayRPC() && response != null) {
                            response.setOpaque(opaque);
                            addTransferFields(response, cmd);
                            response.markResponseType();

                            try {
                                ctx.writeAndFlush(response);
                            } catch (Throwable var3) {
                                logger.error("process request over, but response failed", var3);
                                logger.error(cmd.toString());
                                logger.error(response.toString());
                            }
                        }
                    } catch (Throwable var4) {
                        logger.error("process request exception", var4);
                        logger.error(cmd.toString());
                        if (!cmd.isOnewayRPC()) {
                            RemotingCommand responsex = RemotingCommand.createResponseCommand(1, RemotingHelper.exceptionSimpleDesc(var4));
                            responsex.setOpaque(opaque);
                            addTransferFields(responsex, cmd);
                            ctx.writeAndFlush(responsex);
                        }
                    }

                }
            };
            if (pair.getObject1().rejectRequest()) {
                response = RemotingCommand.createResponseCommand(2, "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                addTransferFields(response, cmd);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException var9) {
                if (System.currentTimeMillis() % 10000L == 0L) {
                    logger.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + ", too many requests and system thread pool busy, RejectedExecutionException " + pair.getObject2().toString() + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    response = RemotingCommand.createResponseCommand(2, "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    addTransferFields(response, cmd);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            response = RemotingCommand.createResponseCommand(3, error);
            response.setOpaque(opaque);
            addTransferFields(response, cmd);
            ctx.writeAndFlush(response);
            logger.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }


    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        sender.processResponseCommand(ctx, cmd);
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (receiver.getRPCHooks().size() > 0) {
            Iterator var3 = receiver.getRPCHooks().iterator();

            while (var3.hasNext()) {
                RPCHook rpcHook = (RPCHook) var3.next();
                rpcHook.doBeforeRequest(addr, request);
            }
        }

    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (receiver.getRPCHooks().size() > 0) {
            Iterator var4 = receiver.getRPCHooks().iterator();

            while (var4.hasNext()) {
                RPCHook rpcHook = (RPCHook) var4.next();
                rpcHook.doAfterResponse(addr, request, response);
            }
        }

    }

    public class Sender extends NettyRemotingClient {

        public Sender(NettyClientConfig nettyClientConfig) {
            super(nettyClientConfig);
        }

        @Override
        public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            NodeTransfer.this.processMessageReceived(ctx, msg);
        }
    }


    public class Receiver extends NettyRemotingServer {

        public Receiver(NettyServerConfig nettyServerConfig) {
            super(nettyServerConfig);
        }

        @Override
        public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            NodeTransfer.this.processMessageReceived(ctx, msg);
        }
    }
}
