package nodetransferwrapper;

import com.duansx.nodetransfer.client.DefaultFileStorage;
import com.duansx.nodetransfer.client.DiscardBusHandler;
import com.duansx.nodetransfer.client.NodeTransferWrapper;
import com.duansx.nodetransfer.core.route.Edge;
import com.duansx.nodetransfer.core.route.Node;
import com.duansx.nodetransfer.core.route.ShortestPathRouteServer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class NodeTransferWrapper3000 {

    public static final String CURRENT_NODE_CODE = "3000";
    public static final int LISTEN_PORT = 3000;
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

    public static void main(String[] args) {
        NodeTransferWrapper instance = getInstance();
        instance.start();
    }
}
