package edu.agh.streamgraph.dynsssp;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class ProcessMessage {
    public ProcessEvent eventType;
    public Integer vertexId;
    public Integer vertexEndpoint;
    public Integer distance;

    public ProcessMessage() {
    }

    public ProcessMessage(ProcessEvent eventType, Integer vertexId, Integer vertexEndpoint, Integer distance) {
        this.eventType = eventType;
        this.vertexId = vertexId;
        this.vertexEndpoint = vertexEndpoint;
        this.distance = distance;
    }


    public static ProcessMessage forVertexAddition(Edge<Integer, NullValue> edge) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.ADD_VERTEX;
        pm.vertexId = edge.f0;
        pm.vertexEndpoint = edge.f1;
        return pm;
    }

    public static ProcessMessage forDistanceUpdate(Integer vertexId, Integer vertexEndpoint, Integer distance) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.UPDATE_DISTANCE;
        pm.vertexId = vertexId;
        pm.vertexEndpoint = vertexEndpoint;
        pm.distance = distance;
        return pm;
    }

    public static ProcessMessage forDistanceRequest(Integer vertexId, Integer vertexEndpoint) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.REQUEST_DISTANCE;
        pm.vertexId = vertexId;
        pm.vertexEndpoint = vertexEndpoint;
        return pm;
    }

    public static ProcessMessage forVertexOutgoing(Integer vertexId, Integer vertexEndpoint, Integer distance) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.VERTEX_OUTGOING;
        pm.vertexId = vertexId;
        pm.vertexEndpoint = vertexEndpoint;
        pm.distance = distance;
        return pm;
    }

    @Override
    public String toString() {
        return "ProcessMessage{" +
                "eventType=" + eventType +
                ", vertexId=" + vertexId +
                ", vertexEndpoint=" + vertexEndpoint +
                ", distance=" + distance +
                '}';
    }
}
