package edu.agh.streamgraph.dynamicconnectivitycomponents;

public class InternalMessage {
    public Long vertexId;
    public Long componentId;

    public InternalMessage() {}
    public InternalMessage(Long vertexId, Long componentId) {
        this.vertexId = vertexId;
        this.componentId = componentId;
    }

    @Override
    public String toString() {
        return "Msg(" + vertexId + " <- " + componentId + ")";
    }
}