package edu.agh.streamgraph.dynamicconnectedcomponents;

public class Edge {
    public Long source;
    public Long target;

    public Edge() {}
    public Edge(Long source, Long target) {
        this.source = source;
        this.target = target;
    }

    @Override
    public String toString() {
        return "(" + source + ", " + target + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return (source.equals(edge.source) && target.equals(edge.target)) ||
                (source.equals(edge.target) && target.equals(edge.source)); // Consider undirected
    }

    @Override
    public int hashCode() {
        return Long.hashCode(source) + Long.hashCode(target); // Simple hash for undirected
    }
}