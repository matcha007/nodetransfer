package com.duansx.nodetransfer.client;

import com.alibaba.fastjson.JSONObject;
import com.duansx.nodetransfer.core.NodeTransfer;
import com.duansx.nodetransfer.core.NodeTransferConfig;
import com.duansx.nodetransfer.core.RouteServer;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Objects;

public class NodeTransferWrapper {

    private static final Logger logger = LoggerFactory.getLogger(NodeTransferWrapper.class);
    private final NodeTransfer nodeTransfer;
    private final HashMap<String, BusHandler<?>> busHandlerTable = new HashMap<>(64);
    private FileStorage fileStorage;

    public NodeTransferWrapper(String currentNodeCode) {
        this(new NodeTransferConfig(currentNodeCode), null);
    }

    public NodeTransferWrapper(String currentNodeCode, RouteServer routeServer, int listenPort, FileStorage fileStorage) {
        this(new NodeTransferConfig(currentNodeCode, routeServer, listenPort), fileStorage);
    }

    public NodeTransferWrapper(NodeTransferConfig nodeTransferConfig, FileStorage fileStorage) {
        this.nodeTransfer = new NodeTransfer(nodeTransferConfig);
        this.fileStorage = fileStorage;
        this.nodeTransfer.registerProcessor(1, new NodeTransferProcessor(), null);
        this.nodeTransfer.registerProcessor(2, new FileRemotingProcessor(fileStorage), null);
        this.nodeTransfer.registerProcessor(3, new FileRemotingProcessor(fileStorage), null);
    }


    public void start() {
        nodeTransfer.start();
    }

    public void shutdown() {
        nodeTransfer.shutdown();
    }


    public NodeTransfer getNodeTransfer() {
        return nodeTransfer;
    }

    public FileStorage getFileStorage() {
        return fileStorage;
    }

    public void setFileStorage(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
    }

    public HashMap<String, BusHandler<?>> getBusHandlerTable() {
        return busHandlerTable;
    }

    public void registerBusHandler(String busCode, BusHandler<?> busHandler) {
        this.busHandlerTable.put(busCode, busHandler);
    }

    public void sendOneway(NodeTransferHeader nodeTransferHeader, Object msg, long timeoutMillis) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(1, nodeTransferHeader);
        request.setBody(JSONObject.toJSONBytes(msg));
        nodeTransfer.invokeOneway(nodeTransferHeader.getTargetNodeCode(), request, timeoutMillis);
    }

    public Object sendSync(NodeTransferHeader nodeTransferHeader, Object msg, long timeoutMillis) throws Exception {
        return sendSync(nodeTransferHeader, msg, timeoutMillis, Object.class);
    }

    public <T> T sendSync(NodeTransferHeader nodeTransferHeader, Object msg, long timeoutMillis, Class<T> clazz) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(1, nodeTransferHeader);
        request.setBody(JSONObject.toJSONBytes(msg));
        RemotingCommand response = nodeTransfer.invokeSync(nodeTransferHeader.getTargetNodeCode(), request, timeoutMillis);
        if (response.getCode() != 0) {
            throw new RemotingCommandException(response.getRemark());
        }
        return response.getBody() == null ? null : JSONObject.parseObject(response.getBody(), clazz);
    }

    public void sendAsync(NodeTransferHeader nodeTransferHeader, Object msg, long timeoutMillis, NodeTransferCallBack<?> nodeTransferCallBack) throws Exception {
        NodeTransferInvokeCallback nodeTransferInvokeCallback = new NodeTransferInvokeCallback(nodeTransferCallBack);
        RemotingCommand request = RemotingCommand.createRequestCommand(1, nodeTransferHeader);
        request.setBody(JSONObject.toJSONBytes(msg));
        nodeTransfer.invokeAsync(nodeTransferHeader.getTargetNodeCode(), request, timeoutMillis, nodeTransferInvokeCallback);
    }

    public Object uploadFileSync(String targetNodeCode, FileHeader fileHeader, long timeoutMillis) throws Exception {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request = RemotingCommand.createRequestCommand(2, fileHeader);
        File file = fileStorage.get(fileHeader.getLocalFilePath());
        byte[] read = fileStorage.read(file, fileHeader.getOffset(), fileHeader.getChunkSize(), fileHeader.getLocalDoEncode());
        request.setBody(read);
        RemotingCommand response = nodeTransfer.invokeSync(targetNodeCode, request, timeoutMillis);
        if (response.getCode() != 0) {
            throw new RemotingCommandException(response.getRemark());
        }

        FileHeader responseHeader = (FileHeader) response.decodeCommandCustomHeader(FileHeader.class);
        if (responseHeader.getOffset() < responseHeader.getFileSize()) {
            fileHeader.setOffset(responseHeader.getOffset());
            long costTime = System.currentTimeMillis() - beginStartTime;
            return uploadFileSync(targetNodeCode, fileHeader, timeoutMillis - costTime);
        }

        return response.getBody() == null ? null : JSONObject.parseObject(response.getBody(), Object.class);
    }


    public void uploadFileAsync(String targetNodeCode, FileHeader fileHeader, long timeoutMillis, NodeTransferCallBack<?> nodeTransferCallBack) throws Exception {
        long beginStartTime = System.currentTimeMillis();

        NodeTransferInvokeCallback nodeTransferInvokeCallback = new NodeTransferInvokeCallback(nodeTransferCallBack) {
            @Override
            public void handle(RemotingCommand responseCommand) throws Exception {
                FileHeader responseHeader = (FileHeader) responseCommand.decodeCommandCustomHeader(FileHeader.class);
                if (responseHeader.getOffset() < responseHeader.getFileSize()) {
                    responseHeader.setOffset(responseHeader.getOffset());
                    RemotingCommand request = RemotingCommand.createRequestCommand(2, responseHeader);
                    File file = fileStorage.get(fileHeader.getLocalFilePath());
                    byte[] read = fileStorage.read(file, responseHeader.getOffset(), responseHeader.getChunkSize(), fileHeader.getLocalDoEncode());
                    request.setBody(read);
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    nodeTransfer.invokeAsync(targetNodeCode, request, timeoutMillis - costTime, this);
                }else {
                    super.handle(responseCommand);
                }
            }
        };


        RemotingCommand request = RemotingCommand.createRequestCommand(2, fileHeader);
        File file = fileStorage.get(fileHeader.getLocalFilePath());
        byte[] read = fileStorage.read(file, fileHeader.getOffset(), fileHeader.getChunkSize(), fileHeader.getLocalDoEncode());
        request.setBody(read);
        nodeTransfer.invokeAsync(targetNodeCode, request, timeoutMillis, nodeTransferInvokeCallback);
    }

    public void downloadFileSync(String targetNodeCode, FileHeader fileHeader, long timeoutMillis) throws Exception {
        long beginStartTime = System.currentTimeMillis();

        File dstFile = fileStorage.get(fileHeader.getLocalFilePath());
        if (dstFile.exists()) {
            logger.warn("fileName[{}]--already exist", dstFile.getAbsoluteFile());
            return;
        }

        File tempFile = new File(dstFile.getAbsolutePath() + ".tmp");
        fileHeader.setOffset(tempFile.length());

        RemotingCommand request = RemotingCommand.createRequestCommand(3, fileHeader);
        RemotingCommand response = nodeTransfer.invokeSync(targetNodeCode, request, timeoutMillis);
        if (response.getCode() != 0) {
            throw new RemotingCommandException(response.getRemark());
        }

        FileHeader responseHeader = (FileHeader) response.decodeCommandCustomHeader(FileHeader.class);
        byte[] body = response.getBody();
        fileStorage.write(tempFile, fileHeader.getOffset(), body, fileHeader.getLocalDoEncode());

        if (tempFile.length() >= responseHeader.getFileSize()) {
            if (!tempFile.renameTo(dstFile)) {
                Files.copy(tempFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                tempFile.delete();
            }
        }else {
            long costTime = System.currentTimeMillis() - beginStartTime;
            downloadFileSync(targetNodeCode, fileHeader, timeoutMillis - costTime);
        }
    }

    public void downloadFileAsync(String targetNodeCode, FileHeader fileHeader, long timeoutMillis, NodeTransferCallBack<?> nodeTransferCallBack) throws Exception {
        long beginStartTime = System.currentTimeMillis();

        File dstFile = fileStorage.get(fileHeader.getLocalFilePath());
        if (dstFile.exists()) {
            logger.warn("fileName[{}]--already exist", dstFile.getAbsoluteFile());
            return;
        }

        File tempFile = new File(dstFile.getAbsolutePath() + ".tmp");
        fileHeader.setOffset(tempFile.length());

        NodeTransferInvokeCallback nodeTransferInvokeCallback = new NodeTransferInvokeCallback(nodeTransferCallBack) {
            @Override
            public void handle(RemotingCommand responseCommand) throws Exception {
                FileHeader responseHeader = (FileHeader) responseCommand.decodeCommandCustomHeader(FileHeader.class);
                byte[] body = responseCommand.getBody();
                fileStorage.write(tempFile, fileHeader.getOffset(), body, fileHeader.getLocalDoEncode());
                fileHeader.setOffset(tempFile.length());

                if (tempFile.length() >= responseHeader.getFileSize()) {
                    if (!tempFile.renameTo(dstFile)) {
                        Files.copy(tempFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                        tempFile.delete();
                    }

                    super.handle(responseCommand);
                }else {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    RemotingCommand request = RemotingCommand.createRequestCommand(3, fileHeader);
                    nodeTransfer.invokeAsync(targetNodeCode, request, timeoutMillis - costTime, this);
                }
            }
        };


        RemotingCommand request = RemotingCommand.createRequestCommand(3, fileHeader);
        nodeTransfer.invokeAsync(targetNodeCode, request, timeoutMillis, nodeTransferInvokeCallback);
    }

    private class NodeTransferInvokeCallback implements InvokeCallback {

        private final NodeTransferCallBack<?> nodeTransferCallBack;

        public NodeTransferInvokeCallback(NodeTransferCallBack<?> nodeTransferCallBack) {
            this.nodeTransferCallBack = nodeTransferCallBack;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            RemotingCommand responseCommand = responseFuture.getResponseCommand();
            try {
                if (null == responseCommand) {
                    if (responseFuture.isSendRequestOK()) {
                        throw new RemotingTimeoutException(RemotingHelper.parseChannelRemoteAddr(responseFuture.getProcessChannel()), responseFuture.getTimeoutMillis(), responseFuture.getCause());
                    }

                    throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(responseFuture.getProcessChannel()), responseFuture.getCause());
                } else {
                    if (responseCommand.getCode() != 0) {
                        HashMap<String, String> extFields = responseCommand.getExtFields();
                        String sourceNodeCode = extFields.get(NodeTransfer.SOURCE_NODE_CODE);
                        throw new Exception("node " + sourceNodeCode + " error : " + responseCommand.getRemark());
                    }

                    handle(responseCommand);
                }
            } catch (Exception e) {
                nodeTransferCallBack.onException(e);
            }
        }

        public void handle(RemotingCommand responseCommand) throws Exception {
            invoke(nodeTransferCallBack, NodeTransferCallBack.class, "T", "onSuccess", responseCommand.getBody());
        };
    }

    protected Object invoke(Object object, Class<?> parametrizedInterface, String typeParamName, String methodName, byte[] body) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<?> t = find(object, parametrizedInterface, typeParamName);
        Object result = body == null ? null : JSONObject.parseObject(body, t);
        Class<?> targetClazz = object.getClass();
        Method method = targetClazz.getMethod(methodName, t);
        method.setAccessible(true);
        return method.invoke(object, result);
    }

    protected static Class<?> find(Object object, Class<?> parametrizedInterface, String typeParamName) {
        Class<?> thisClass = object.getClass();
        while (thisClass != null) {
            Type[] interfaces = thisClass.getGenericInterfaces();

            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), parametrizedInterface)) {
                        int typeParamIndex = -1;
                        TypeVariable<?>[] typeParams = parametrizedInterface.getTypeParameters();

                        for (int i = 0; i < typeParams.length; ++i) {
                            if (typeParamName.equals(typeParams[i].getName())) {
                                typeParamIndex = i;
                                break;
                            }
                        }

                        if (typeParamIndex < 0) {
                            throw new IllegalStateException("unknown type parameter '" + typeParamName + "': " + parametrizedInterface);
                        }

                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        Type actualTypeParam = actualTypeArguments[typeParamIndex];
                        if (actualTypeParam instanceof ParameterizedType) {
                            actualTypeParam = ((ParameterizedType) actualTypeParam).getRawType();
                        }

                        if (actualTypeParam instanceof Class) {
                            return (Class<?>) actualTypeParam;
                        }
                    }
                }
            }

            thisClass = thisClass.getSuperclass();
        }

        return Object.class;
    }

    public Object dispatch(String targetBusCode, byte[] body) throws Exception {
        BusHandler<?> busHandler = busHandlerTable.get(targetBusCode);
        if (busHandler == null) {
            throw new Exception(" bus code " + targetBusCode + " not supported");
        }

        return invoke(busHandler, BusHandler.class, "T", "receive", body);
    }


    public class NodeTransferProcessor implements NettyRequestProcessor {
        @Override
        public RemotingCommand processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand request) throws Exception {
            RemotingCommand response = RemotingCommand.createResponseCommand(0, (String) null);
            response.setOpaque(request.getOpaque());

            NodeTransferHeader nodeTransferHeader = (NodeTransferHeader) request.decodeCommandCustomHeader(NodeTransferHeader.class);
            String targetBusCode = nodeTransferHeader.getTargetBusCode();

            Object object = NodeTransferWrapper.this.dispatch(targetBusCode, request.getBody());
            response.setBody(object != null ? JSONObject.toJSONBytes(object) : null);

            return response;
        }

        @Override
        public boolean rejectRequest() {
            return false;
        }
    }


}
