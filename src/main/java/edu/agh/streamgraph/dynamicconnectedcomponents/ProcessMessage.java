package edu.agh.streamgraph.dynamicconnectedcomponents;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class ProcessMessage {
    public ProcessEvent eventType;
    public Long vertexId;
    public Edge<Long, NullValue> edge;
    public InternalMessage internalMessage;


    public static ProcessMessage forEdgeIncoming(Edge<Long, NullValue> edge) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.EDGE_INCOMING;
        pm.vertexId = edge.f0;
        pm.edge = edge;
        return pm;
    }

    public static ProcessMessage forEdgeOutgoing(Edge<Long, NullValue> edge, InternalMessage cm) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.EDGE_OUTGOING;
        pm.vertexId = edge.f0;
        pm.edge = edge;
        pm.internalMessage = cm;
        return pm;
    }

    public static ProcessMessage forInternalMessage(InternalMessage cm) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.INTERNAL_MESSAGE;
        pm.vertexId = cm.vertexId;
        pm.internalMessage = cm;
        return pm;
    }

    @Override
    public String toString() {
        switch (eventType) {
            case EDGE_INCOMING:
                return "ProcessMessage(ADD_EDGE, " + edge + ")";
            case INTERNAL_MESSAGE:
                return "ProcessMessage(COMPONENT_MESSAGE, " + internalMessage + ")";
            default:
                return "ProcessMessage(UNKNOWN)";
        }
    }
}
