package dispatcher;

import cn.hutool.core.collection.ConcurrentHashSet;
import com.swcc.nodetransfer.client.*;
import com.swcc.nodetransfer.client.dispatcher.*;
import com.swcc.nodetransfer.core.NodeTransfer;
import com.swcc.nodetransfer.core.NodeTransferConfig;
import com.swcc.nodetransfer.core.route.Edge;
import com.swcc.nodetransfer.core.route.Node;
import com.swcc.nodetransfer.core.route.ShortestPathRouteServer;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodeMessageDispatcher implements Dispatcher {

    private static final Logger logger = LoggerFactory.getLogger(NodeMessageDispatcher.class);

    private final ConcurrentHashSet<String> targets = new ConcurrentHashSet<>();
    private final DispatchImpl dispatchImpl = new DispatchImpl(this);
    private final DispatcherFactory dispatcherFactory;
    private final NodeTransferWrapper nodeTransferWrapper;
    private final ConcurrentLinkedQueue<String> store = new ConcurrentLinkedQueue<>();

    public NodeMessageDispatcher(DispatcherFactory dispatcherFactory, NodeTransferWrapper nodeTransferWrapper) {
        this.dispatcherFactory = dispatcherFactory;
        this.nodeTransferWrapper = nodeTransferWrapper;


        File dir = new File("C:\\Users\\duansx055016\\Desktop\\横向\\1000");
        for (File file : dir.listFiles()) {
            store.add(file.getAbsolutePath());
        }

//        store.add("C:\\Users\\duansx055016\\Desktop\\横向\\1000\\ideaIU-2019.3.1.exe");

    }

    @Override
    public String name() {
        return "nodeMessageDispatcher";
    }

    @Override
    public void pushMessage(PushRequest pushRequest) {
        String targetNodeCode = pushRequest.getTarget();
        PushMessageService pushMessageService = dispatcherFactory.getPushMessageService();
        if (!targetNodeCode.equals("2000")) {
            pushMessageService.executePushRequestLater(pushRequest,5000L);
            return;
        }

        List<String> stringList = new ArrayList<>();
        for (int i = 0; !store.isEmpty() && i < 1000; i++) {
            stringList.add(store.poll());
        }


        for (int i = 0; i < stringList.size(); i++) {
            try {
                String str = stringList.get(i);
                File file = new File(str);
                FileHeader fileHeader = new FileHeader(file);
                fileHeader.setRemoteFilePath(file.getName());
                NodeTransferCallBack<?> nodeTransferCallBack = new NodeTransferCallBack<Object>() {
                    @Override
                    public void onSuccess(Object o) {
                        try {
                            nodeTransferWrapper.sendSync(new NodeTransferHeader("2000", "discardBus"), str, 5000);
                        } catch (Exception e) {
                            store.add(str);
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        store.add(str);
                        throw new RuntimeException(throwable);
                    }
                };
                nodeTransferWrapper.uploadFileAsync("2000", fileHeader, 60000,nodeTransferCallBack);
            } catch (Exception e) {
                for (int j = i; j < stringList.size(); j++) {
                    String str = stringList.get(i);
                    store.add(str);
                }

                pushMessageService.executePushRequestLater(pushRequest,5000L);
                throw new RuntimeException(e);
            }
        }

        pushMessageService.executePushRequestImmediately(pushRequest);
    }



    @Override
    public void updateTargets() {
        targets.clear();
        NodeTransfer nodeTransfer = nodeTransferWrapper.getNodeTransfer();
        ShortestPathRouteServer routeServer = (ShortestPathRouteServer) nodeTransfer.getRouteServer();
        Map<Node, List<Node>> graph = routeServer.getGraph();
        for (Node node : graph.keySet()) {
            if (!node.getName().equals(nodeTransfer.getCurrentNodeCode())) {
                List<Node> nextNode = routeServer.getNextNodes(nodeTransfer.getCurrentNodeCode(), node.getName());
                for (Node next : nextNode) {
                    targets.add(next.getName());
                }
            }
        }

        ConcurrentHashSet<String> targets = this.dispatchImpl.getTargets();
        targets.addAll(this.targets);
    }

    @Override
    public void doDispatch() {
        dispatchImpl.doDispatch();
    }

    public static void main(String[] args) {
        DispatcherFactory dispatcherFactory = new DispatcherFactory();

        List<Edge> list = new ArrayList<>();
        list.add(new Edge(new Node("1000", "127.0.0.1:1000"), new Node("2000", "10.0.85.20:2000")));
//        list.add(new Edge(new Node("2000", "10.0.80.88:2000"), new Node("3000", "127.0.0.1:3000")));
//        list.add(new Edge(new Node("3000", "127.0.0.1:3000"), new Node("4000", "127.0.0.1:4000")));
        ShortestPathRouteServer routeStorage = new ShortestPathRouteServer(list);
        NodeTransferConfig nodeTransferConfig = new NodeTransferConfig("1000", routeStorage, 1000);
        NettyClientConfig nettyClientConfig = nodeTransferConfig.getNettyClientConfig();
        nettyClientConfig.setClientAsyncSemaphoreValue(100);
        nettyClientConfig.setClientOnewaySemaphoreValue(100);
        NodeTransferWrapper nodeTransfer = new NodeTransferWrapper(nodeTransferConfig, new DefaultFileStorage(new File("C:\\Users\\duansx055016\\Desktop\\横向\\1000")));
        nodeTransfer.registerBusHandler("discardBus",new DiscardBusHandler());

        NodeMessageDispatcher nodeMessageDispatcher = new NodeMessageDispatcher(dispatcherFactory, nodeTransfer);

        dispatcherFactory.registerDispatcher(nodeMessageDispatcher.name(), nodeMessageDispatcher);
        nodeTransfer.start();
        dispatcherFactory.start();

    }
}
