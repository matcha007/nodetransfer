package nodetransferwrapper;

import com.duansx.nodetransfer.client.*;
import com.duansx.nodetransfer.core.route.Edge;
import com.duansx.nodetransfer.core.route.Node;
import com.duansx.nodetransfer.core.route.ShortestPathRouteServer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class NodeTransferWrapper1000 {

    public static final String CURRENT_NODE_CODE = "1000";
    public static final int LISTEN_PORT = 1000;
    private static NodeTransferWrapper nodeTransfer;

    public static synchronized NodeTransferWrapper getInstance() {
        if (nodeTransfer == null) {
            synchronized (NodeTransferWrapper1000.class){
                List<Edge> list = new ArrayList<>();
                list.add(new Edge(new Node("1000", "127.0.0.1:1000"), new Node("2000", "127.0.0.1:2000")));
                list.add(new Edge(new Node("2000", "127.0.0.1:2000"), new Node("3000", "127.0.0.1:3000")));
                list.add(new Edge(new Node("3000", "127.0.0.1:3000"), new Node("4000", "127.0.0.1:4000")));
                ShortestPathRouteServer routeStorage = new ShortestPathRouteServer(list);
                nodeTransfer = new NodeTransferWrapper(CURRENT_NODE_CODE, routeStorage, LISTEN_PORT, new DefaultFileStorage(new File("C:\\Users\\duansx055016\\Desktop\\横向\\" + CURRENT_NODE_CODE)));
                nodeTransfer.registerBusHandler("discardBus",new DiscardBusHandler());
            }
        }

        return nodeTransfer;
    }


    public static void main(String[] args) throws Exception {
        NodeTransferWrapper instance = getInstance();
        instance.start();

//        testSendTextAsync();
//        testSendTextAsync();
//        testSendTextOneway();
//        testSendFileSync("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\ideaIU-2019.3.1.exe");
//        testSendFileSync("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\diep172841933748404546");
//        testSendFileAsync("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\ideaIU-2019.3.1.exe");
        FileHeader fileHeader = new FileHeader(new File("C:\\Users\\duansx055016\\Desktop\\横向\\2000\\ideaIU-2019.3.1.exe"));
        fileHeader.setRemoteFilePath("C:\\Users\\duansx055016\\Desktop\\横向\\4000\\aaaa\\ideaIU-2019.3.1.exe");
//        testDownloadFileSync(fileHeader);
        testDownloadFileAsync(fileHeader);
    }

    public static void testSendTextSync() throws Exception {
        String o = getInstance().sendSync(new NodeTransferHeader("4000", "discardBus"), "hello", 10000L, String.class);
        System.out.println(o);
    }

    public static void testSendTextAsync() throws Exception {
        NodeTransferCallBack<String> nodeTransferCallBack = new NodeTransferCallBack<String>() {
            @Override
            public void onSuccess(String object) {
                System.out.println(object);
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }
        };
        getInstance().sendAsync(new NodeTransferHeader("4000", "discardBus"), "hello", 10000L, nodeTransferCallBack);

    }

    public static void testSendTextOneway() throws Exception {
        getInstance().sendOneway(new NodeTransferHeader("4000", "discardBus"), "hello", 10000L);
    }

    public static void testSendFileSync(String filePath) throws Exception {
        long start = System.currentTimeMillis();
        System.out.println();
        File file = new File(filePath);
        FileHeader fileHeader = new FileHeader(file);
        fileHeader.setLocalDoEncode(false);
        fileHeader.setRemoteFilePath("aaaa\\" + file.getName());
        Object o = getInstance().uploadFileSync("4000", fileHeader, 60000L);
        System.out.println(o);

        long end = System.currentTimeMillis();
        System.out.println(start-end);
    }

    public static void testSendFileAsync(String filePath) throws Exception {
        File file = new File(filePath);
        FileHeader fileHeader = new FileHeader(file);
        fileHeader.setRemoteFilePath(file.getName());
        NodeTransferCallBack<?> nodeTransferCallBack = new NodeTransferCallBack<Object>() {
            @Override
            public void onSuccess(Object object) {
                System.out.println(object);
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }
        };
        getInstance().uploadFileAsync("4000", fileHeader, 60000L, nodeTransferCallBack);
    }

    public static void testDownloadFileSync(FileHeader fileHeader) throws Exception {
        getInstance().downloadFileSync("4000", fileHeader, 60000L);
    }

    public static void testDownloadFileAsync(FileHeader fileHeader) throws Exception {
        System.out.println("1");
        NodeTransferCallBack<?> nodeTransferCallBack = new NodeTransferCallBack<Object>() {
            @Override
            public void onSuccess(Object object) {
                System.out.println("下载成功");
                System.out.println(object);
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println(throwable.getMessage());
            }
        };
        getInstance().downloadFileAsync("4000", fileHeader, 60000L, nodeTransferCallBack);
        System.out.println("2");
    }
}
