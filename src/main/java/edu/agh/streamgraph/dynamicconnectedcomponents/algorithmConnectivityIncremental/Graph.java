package edu.agh.streamgraph.dynamicconnectedcomponents.algorithmConnectivityIncremental;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

public class Graph implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Map<Integer, Vertex> vertices = new HashMap<>();
    private LinkedHashSet<Integer> likedSet = new LinkedHashSet<>();

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Graph graph = new Graph();
        graph.addVertex(1);
        ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("graph.ser"));
        out.writeObject(graph);
        out.close();

        System.out.println("Works!");
        ObjectInputStream in = new ObjectInputStream(new FileInputStream("graph.ser"));
        Graph graph1 = (Graph) in.readObject();
        in.close();

    }

    public Graph(){
        likedSet.add(1);
        likedSet.add(2);
        likedSet.remove(1);
    }

    public void addVertex(Vertex vertex){
        if(!this.vertices.containsKey(vertex.getId()))
            this.vertices.put(vertex.getId(), vertex);
        else
            throw new RuntimeException("Vertex already exists");
    }

    public void addVertex(Integer vertexId){
        if(!this.vertices.containsKey(vertexId))
            this.vertices.put(vertexId, new Vertex(vertexId));
        else
            throw new RuntimeException("Vertex already exists");
    }

    public void addNonDirectedEdge(Vertex vertexFrom, Vertex vertexTo){

        if(!this.vertices.containsKey(vertexFrom.getId()))
            this.vertices.put(vertexFrom.getId(), vertexFrom);

        if(!this.vertices.containsKey(vertexTo.getId()))
            this.vertices.put(vertexTo.getId(), vertexTo);

        this.addDirectedEdge(vertexFrom, vertexTo);
        this.addDirectedEdge(vertexTo, vertexFrom);
    }

    public void addNonDirectedEdge(int idVertexFrom, int idVertexTo){
        Vertex vertexFrom;
        Vertex vertexTo;
        if(this.vertices.containsKey(idVertexFrom))
            vertexFrom = this.vertices.get(idVertexFrom);
        else vertexFrom = new Vertex(idVertexFrom);
        if(this.vertices.containsKey(idVertexTo))
            vertexTo = this.vertices.get(idVertexTo);
        else vertexTo = new Vertex(idVertexTo);
        this.addNonDirectedEdge(vertexFrom, vertexTo);
    }

    public void addDirectedEdge(Vertex beginVertex, Vertex endVertex){
        beginVertex.addEdgeTo(endVertex);
        Vertex end = this.findRoot(endVertex);
        Vertex begin  = this.findRoot(beginVertex);

        if(!end.equals(begin)){
            Integer rankEnd = end.getRank();
            Integer rankBegin = begin.getRank();
            if(rankEnd < rankBegin){
                end.setIdSet(begin.getIdSet());
            } else if(rankEnd > rankBegin){
                begin.setIdSet(end.getIdSet());
            } else {
                begin.setRank(begin.getRank() + 1);
                end.setIdSet(begin.getIdSet());
            }
        }

    }

    public Vertex findRoot(Vertex parent){
        while(!parent.getIdSet().equals(parent.getId())) {
            parent = this.vertices.get(parent.getIdSet());
        }
        return parent;
    }

    public boolean isConnected(int idVertexFrom, int idVertexTo){
        Integer rootFrom;
        Integer rootTo;
        if(this.vertices.containsKey(idVertexFrom))
            rootFrom = this.findRoot(this.vertices.get(idVertexFrom)).getIdSet();
        else throw new RuntimeException(String.format("Vertex %d does not exist", idVertexFrom));
        if(this.vertices.containsKey(idVertexTo))
            rootTo = this.findRoot(this.vertices.get(idVertexTo)).getIdSet();
        else throw new RuntimeException(String.format("Vertex %d does not exist", idVertexTo));

        return rootTo.equals(rootFrom);

    }

}
