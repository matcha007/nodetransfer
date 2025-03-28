package nodetransfer;

import com.duansx.nodetransfer.client.DefaultFileStorage;
import com.duansx.nodetransfer.client.DiscardRemotingProcessor;
import com.duansx.nodetransfer.client.FileRemotingProcessor;
import com.duansx.nodetransfer.core.NodeTransfer;
import com.duansx.nodetransfer.core.NodeTransferConfig;
import com.duansx.nodetransfer.core.route.Edge;
import com.duansx.nodetransfer.core.route.Node;
import com.duansx.nodetransfer.core.route.ShortestPathRouteServer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class NodeTransfer2000 {

    private static NodeTransfer nodeTransfer;
    public static final String CURRENT_NODE_CODE = "2000";
    public static final int LISTEN_PORT = 2000;

    public static synchronized NodeTransfer getInstance() {
        if (nodeTransfer == null) {
            synchronized (NodeTransfer1000.class){
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

    public static void main(String[] args) {
        NodeTransfer instance = getInstance();
        instance.start();
    }
}
