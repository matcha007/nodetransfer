package com.swcc.nodetransfer.core.route;

import java.util.*;

public class ShortestPathInGraph {

    public static List<Node> shortestPath(Map<Node, List<Node>> graph, Node start, Node end) {
        if (graph == null || start == null || end == null) {
            return new ArrayList<>();
        }

        Queue<Node> queue = new LinkedList<>();
        queue.offer(start);
        Map<Node, Node> parent = new HashMap<>();
        parent.put(start, null);

        while (!queue.isEmpty()) {
            Node node = queue.poll();
            if (node.equals(end)) {
                break;
            }
            for (Node neighbor : graph.getOrDefault(node, Collections.emptyList())) {
                if (!parent.containsKey(neighbor)) {
                    queue.offer(neighbor);
                    parent.put(neighbor, node);
                }
            }
        }

        return reconstructPath(parent, end);
    }


    public static List<Node> shortestPath(List<Edge> edges, Node start, Node end) {
        Map<Node, List<Node>> graph = buildGraph(edges);
        return shortestPath(graph, start, end);
    }

    private static List<Node> reconstructPath(Map<Node, Node> parent, Node end) {
        List<Node> path = new ArrayList<>();
        if (!parent.containsKey(end)) {
            return path;
        }

        Node current = end;
        while (current != null) {
            path.add(current);
            current = parent.get(current);
        }
        Collections.reverse(path);

        return path;
    }

    public static Map<Node, List<Node>> buildGraph(List<Edge> edges) {
        Map<Node, List<Node>> graph = new HashMap<>();
        for (Edge edge : edges) {
            graph.computeIfAbsent(edge.getNode1(), k -> new ArrayList<>()).add(edge.getNode2());
            graph.computeIfAbsent(edge.getNode2(), k -> new ArrayList<>()).add(edge.getNode1());
        }

        return graph;
    }
}
