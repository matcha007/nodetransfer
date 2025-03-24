package com.swcc.nodetransfer.core.route;


import com.swcc.nodetransfer.core.RouteNotFoundException;
import com.swcc.nodetransfer.core.RouteServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ShortestPathRouteServer implements RouteServer {

    private static final Logger logger = LoggerFactory.getLogger(ShortestPathRouteServer.class);

    private List<Edge> edgeList;

    private Map<Node, List<Node>> graph;

    public ShortestPathRouteServer() {
        this(new ArrayList<>());
    }

    public ShortestPathRouteServer(List<Edge> list) {
        this.edgeList = list;
        refreshGraph();
    }

    public Node getNode(String name) {
        for (Map.Entry<Node, List<Node>> nodeListEntry : graph.entrySet()) {
            Node key = nodeListEntry.getKey();
            if (key.getName().equals(name)) {
                return key;
            }
        }

        return null;
    }

    public Node getNextNode(String start, String end) {
        List<Node> nextNodes = getNextNodes(start, end);
        if (!nextNodes.isEmpty()) {
            return nextNodes.get(0);
        }

        return null;
    }

    public List<Node> getNextNodes(String start, String end) {
        List<Node> nodes = ShortestPathInGraph.shortestPath(graph, getNode(start), getNode(end));
//        logger.debug("route of [{}]-->[{}]：[{}]", start, end, nodes.stream().map(Node::getName).collect(Collectors.joining("-->")));
        if (nodes.size() > 1) {
            nodes.remove(0);
            return nodes;
        }

        return new ArrayList<>();
    }

    public List<Edge> getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(List<Edge> edgeList) {
        this.edgeList = edgeList;
        refreshGraph();
    }

    public Map<Node, List<Node>> getGraph() {
        return graph;
    }

    public void add(Edge edge) {
        edgeList.add(edge);
        refreshGraph();
    }

    private void refreshGraph() {
        this.graph = ShortestPathInGraph.buildGraph(edgeList);
    }

    @Override
    public String findNextNodeAddr(String sourceNodeCode, String targetNodeCode) throws RouteNotFoundException {
        Node nextNode = this.getNextNode(sourceNodeCode, targetNodeCode);
        if (nextNode == null) {
            throw new RouteNotFoundException(sourceNodeCode, targetNodeCode);
        }
        return nextNode.getAddr();
    }
}
