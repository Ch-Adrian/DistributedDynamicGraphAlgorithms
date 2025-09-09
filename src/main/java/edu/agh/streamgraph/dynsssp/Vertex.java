package edu.agh.streamgraph.dynsssp;

public class Vertex {
    public Long id;
    public Long distance;
    public Long edgeEndpoint;

    public Vertex() {}

    public Vertex(Long id, Long distance, Long edgeEndpoint) {
        this.id = id;
        this.distance = distance;
        this.edgeEndpoint = edgeEndpoint;
    }

    @Override
    public String toString() {
        return "Vertex(" + id + ", dist=" + distance + ", edgeEndpoint=" + edgeEndpoint + ")";
    }
}
