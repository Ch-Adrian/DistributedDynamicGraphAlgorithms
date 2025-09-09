package edu.agh.streamgraph.dynamicconnectivitycomponents;

public class ProcessMessage {
    public ProcessEvent eventType;
    public Long vertexId;
    public Edge edge;
    public InternalMessage internalMessage;


    public static ProcessMessage forEdgeIncoming(Edge edge) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.EDGE_INCOMING;
        pm.vertexId = edge.source;
        pm.edge = edge;
        return pm;
    }

    public static ProcessMessage forEdgeOutgoing(Edge edge, InternalMessage cm) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.EDGE_OUTGOING;
        pm.vertexId = edge.source;
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
