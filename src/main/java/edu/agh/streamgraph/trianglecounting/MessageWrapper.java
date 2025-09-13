package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class MessageWrapper {

    private final Edge<Integer, NullValue> edge;
    private final MessageState state;
    private Double output;

    public MessageWrapper(Edge<Integer, NullValue> edge, MessageState state) {
        this.edge = edge;
        this.state = state;
        this.output = 0.0;
    }

    public Edge<Integer, NullValue> getEdge() {
        return edge;
    }

    public MessageState getState() {
        return state;
    }

    public Double getOutput() {
        return output;
    }

    public void setOutput(Double output) {
        this.output = output;
    }
}
