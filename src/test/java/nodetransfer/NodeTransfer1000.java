package nodetransfer;

import com.duansx.nodetransfer.client.DefaultFileStorage;
import com.duansx.nodetransfer.client.DiscardRemotingProcessor;
import com.duansx.nodetransfer.client.FileHeader;
import com.duansx.nodetransfer.client.FileRemotingProcessor;
import com.duansx.nodetransfer.core.NodeTransfer;
import com.duansx.nodetransfer.core.NodeTransferConfig;
import com.duansx.nodetransfer.core.RouteNotFoundException;
import com.duansx.nodetransfer.core.route.Edge;
import com.duansx.nodetransfer.core.route.Node;
import com.duansx.nodetransfer.core.route.ShortestPathRouteServer;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.*;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class NodeTransfer1000 {

    private static NodeTransfer nodeTransfer;
    public static final String CURRENT_NODE_CODE = "1000";
    public static final int LISTEN_PORT = 1000;

    public static synchronized NodeTransfer getInstance() {
        if (nodeTransfer == null) {
            synchronized (NodeTransfer1000.class) {
                List<Edge> list = new ArrayList<>();
                list.add(new Edge(new Node("1000", "127.0.0.1:1000"), new Node("2000", "127.0.0.1:2000")));
                list.add(new Edge(new Node("2000", "127.0.0.1:2000"), new Node("3000", "127.0.0.1:3000")));
                list.add(new Edge(new Node("3000", "127.0.0.1:3000"), new Node("4000", "127.0.0.1:4000")));
                ShortestPathRouteServer routeStorage = new ShortestPathRouteServer(list);
                nodeTransfer = new NodeTransfer(new NodeTransferConfig(CURRENT_NODE_CODE, routeStorage, LISTEN_PORT));

                DiscardRemotingProcessor sendRemotingProcessor = new DiscardRemotingProcessor();
                nodeTransfer.registerProcessor(1, sendRemotingProcessor, null);
                FileRemotingProcessor fileRemotingProcessor = new FileRemotingProcessor(new DefaultFileStorage(new File("C:\\Users\\duansx055016\\Desktop\\横向\\" + CURRENT_NODE_CODE)));
                nodeTransfer.registerProcessor(2, fileRemotingProcessor, null);
            }
        }

        return nodeTransfer;
    }

    public static void main(String[] args) throws Exception {
        NodeTransfer instance = getInstance();
        instance.start();

//        testSendTextSync();
//        testSendTextAsync();
//        testSendTextOneway();
        testSendFileSync("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\ideaIU-2019.3.1.exe","aaaa\\ideaIU-2019.3.1.exe");
//        testSendFileAsync("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\ideaIU-2019.3.1.exe");
    }

    public static void testSendTextSync() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(1, null);
        request.setCode(1);
        request.setBody("hello".getBytes(StandardCharsets.UTF_8));

        RemotingCommand response = getInstance().invokeSync("4000", request, 10000L);
        System.out.println(response);
    }


    public static void testSendTextAsync() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException, RemotingTooMuchRequestException {
        InvokeCallback invokeCallback = new InvokeCallback() {
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
                            throw new Exception(sourceNodeCode + ":" + responseCommand.getRemark());
                        } else {
                            System.out.println(responseCommand);
                        }
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        };
        RemotingCommand request = RemotingCommand.createRequestCommand(1, null);
        request.setCode(1);
        request.setBody("hello".getBytes(StandardCharsets.UTF_8));
        getInstance().invokeAsync("4000", request, 10000L, invokeCallback);
    }

    public static void testSendTextOneway() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException, RemotingTooMuchRequestException {
        RemotingCommand request = RemotingCommand.createRequestCommand(1, null);
        request.setCode(1);
        request.setBody("hello".getBytes(StandardCharsets.UTF_8));

        getInstance().invokeOneway("4000", request, 10000L);
    }

    public static void testSendFileSync(String filePath,String targetFilePath) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException, RemotingCommandException, IOException {
        File file = new File(filePath);
        FileHeader fileHeader = new FileHeader();
        fileHeader.setLocalFilePath(file.getName());
        fileHeader.setRemoteFilePath(targetFilePath);
        fileHeader.setFileSize(file.length());
        fileHeader.setChunkSize(1024 * 1024);
        fileHeader.setOffset(0);

        sendFileSync(fileHeader, filePath);
    }

    public static void sendFileSync(FileHeader fileHeader, String filePath) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException, RemotingCommandException, IOException {
        RemotingCommand request = RemotingCommand.createRequestCommand(2, fileHeader);
        request.setBody(read(filePath, fileHeader.getChunkSize(), fileHeader.getOffset()));
        RemotingCommand response = getInstance().invokeSync("4000", request, 10000L);
        if (response.getCode() != 0) {
            throw new RemotingCommandException(response.getRemark());
        }

        FileHeader responseHeader = (FileHeader) response.decodeCommandCustomHeader(FileHeader.class);
        if (responseHeader.getOffset() < responseHeader.getFileSize()) {
            fileHeader.setOffset(responseHeader.getOffset());
            sendFileSync(fileHeader, filePath);
        } else {
            System.out.println("发送完成");
        }
    }

    private static byte[] read(String filePath, long chunkSize, long offset) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(filePath, "r")) {
            // 设置缓冲区大小
            chunkSize = (int) Math.min(chunkSize, randomAccessFile.length() - offset);
            byte[] buf = new byte[(int) chunkSize];

            //从游标开始读取
            randomAccessFile.seek(offset);
            randomAccessFile.read(buf);

            return buf;
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public static void testSendFileAsync(String filePath) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RouteNotFoundException, InterruptedException, RemotingTooMuchRequestException, IOException {
        InvokeCallback invokeCallback = new InvokeCallback() {
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
                            throw new Exception(sourceNodeCode + ":" + responseCommand.getRemark());
                        }

                        FileHeader responseHeader = (FileHeader) responseCommand.decodeCommandCustomHeader(FileHeader.class);
                        if (responseHeader.getOffset() < responseHeader.getFileSize()) {
                            responseHeader.setOffset(responseHeader.getOffset());
                            RemotingCommand request = RemotingCommand.createRequestCommand(2, responseHeader);
                            request.setBody(read(filePath, responseHeader.getChunkSize(), responseHeader.getOffset()));
                            getInstance().invokeAsync("4000", request, 10000L, this);
                        } else {
                            System.out.println("发送完成");
                        }
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        };

        File file = new File(filePath);
        FileHeader fileHeader = new FileHeader();
        fileHeader.setLocalFilePath(file.getName());
        fileHeader.setRemoteFilePath(file.getName());
        fileHeader.setFileSize(file.length());
        fileHeader.setChunkSize(10 * 1024 * 1024);
        fileHeader.setOffset(0);
        RemotingCommand request = RemotingCommand.createRequestCommand(2, fileHeader);
        request.setBody(read(filePath, fileHeader.getChunkSize(), fileHeader.getOffset()));
        getInstance().invokeAsync("4000", request, 10000L, invokeCallback);
    }
}
