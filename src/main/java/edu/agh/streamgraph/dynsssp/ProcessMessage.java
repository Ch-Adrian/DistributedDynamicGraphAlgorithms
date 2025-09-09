package edu.agh.streamgraph.dynsssp;

import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class ProcessMessage {
    public ProcessEvent eventType;
    public Long vertexId;
    public Long vertexEndpoint;
    public Long distance;


    public static ProcessMessage forVertexAddition(Edge<Long, NullValue> edge) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.ADD_VERTEX;
        pm.vertexId = edge.f0;
        pm.vertexEndpoint = edge.f1;
        return pm;
    }

    public static ProcessMessage forDistanceUpdate(Long vertexId, Long vertexEndpoint, Long distance) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.UPDATE_DISTANCE;
        pm.vertexId = vertexId;
        pm.vertexEndpoint = vertexEndpoint;
        pm.distance = distance;
        return pm;
    }

    public static ProcessMessage forDistanceRequest(Long vertexId, Long vertexEndpoint) {
        ProcessMessage pm = new ProcessMessage();
        pm.eventType = ProcessEvent.REQUEST_DISTANCE;
        pm.vertexId = vertexId;
        pm.vertexEndpoint = vertexEndpoint;
        return pm;
    }

    public static ProcessMessage forVertexOutgoing(Long vertexId, Long vertexEndpoint, Long distance) {
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
