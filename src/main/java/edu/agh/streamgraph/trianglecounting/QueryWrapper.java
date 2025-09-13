package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class QueryWrapper {

    private final Edge<Integer, NullValue> edge;
    private final QueryState state;
    private Double output;

    public QueryWrapper(Edge<Integer, NullValue> edge, QueryState state) {
        this.edge = edge;
        this.state = state;
        this.output = 0.0;
    }

    public Edge<Integer, NullValue> getEdge() {
        return edge;
    }

    public QueryState getState() {
        return state;
    }

    public Double getOutput() {
        return output;
    }

    public void setOutput(Double output) {
        this.output = output;
    }
}
