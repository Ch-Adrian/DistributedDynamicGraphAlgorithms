package edu.agh.streamgraph.dynamicconnectedcomponents;

public class InternalMessage {
    public Integer vertexId;
    public Integer componentId;

    public InternalMessage() {}
    public InternalMessage(Integer vertexId, Integer componentId) {
        this.vertexId = vertexId;
        this.componentId = componentId;
    }

    @Override
    public String toString() {
        return "Msg(" + vertexId + " <- " + componentId + ")";
    }
}