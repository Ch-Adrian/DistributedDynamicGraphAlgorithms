package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Objects;

public class DistanceProcessor extends KeyedProcessFunction<Integer, ProcessMessage, ProcessMessage> {

    private ValueState<Integer> vertexId;
    private ValueState<Integer> distanceState;
    private MapState<Integer, Integer> neighborsIds;
    private Integer soureceId = 1;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        distanceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("label", Integer.class)
        );
        neighborsIds = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("neighbors", Integer.class, Integer.class)
        );
        vertexId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("vertexId", Integer.class)
        );
    }

    @Override
    public void processElement(ProcessMessage processMessage,
                               KeyedProcessFunction<Integer, ProcessMessage, ProcessMessage>.Context context,
                               Collector<ProcessMessage> collector) throws Exception {
//        System.out.println("Received message for vertex " + context.getCurrentKey() + ": " + processMessage);
        if(vertexId.value() == null) vertexId.update(context.getCurrentKey());
        if(distanceState.value() == null) distanceState.update(Integer.MAX_VALUE);

        if(processMessage.eventType.equals(ProcessEvent.ADD_VERTEX)) {
//            System.out.println("[ADD] Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
            Integer vertexEnding = processMessage.vertexEndpoint;
            if(!neighborsIds.contains(vertexEnding)) {
                neighborsIds.put(vertexEnding, Integer.MAX_VALUE);
            }
            if(Objects.equals(this.soureceId, processMessage.vertexId)) {
                distanceState.update(0);
                collector.collect(ProcessMessage.forVertexOutgoing(vertexId.value(), vertexEnding, 0));
                for(Integer neighbor : neighborsIds.keys()) {
                    collector.collect(ProcessMessage.forDistanceUpdate(neighbor, vertexId.value(), 0));
                }
            } else {
                if(distanceState.value() != Integer.MAX_VALUE) {
                    collector.collect(ProcessMessage.forDistanceUpdate(vertexEnding, vertexId.value(), distanceState.value()));
                } else {
                    collector.collect(ProcessMessage.forDistanceRequest(vertexEnding, vertexId.value()));
                }
            }
        }
        else if(processMessage.eventType.equals(ProcessEvent.UPDATE_DISTANCE)) {
//            System.out.println("[DIS] Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
            if(processMessage.distance != Integer.MAX_VALUE) {
                Integer newDistance = processMessage.distance + 1;
                if (newDistance < distanceState.value()) {
                    distanceState.update(newDistance);
                    collector.collect(ProcessMessage.forVertexOutgoing(vertexId.value(), processMessage.vertexEndpoint, newDistance));
                    for (Integer neighbor : neighborsIds.keys()) {
                        collector.collect(ProcessMessage.forDistanceUpdate(neighbor, vertexId.value(), distanceState.value()));
                    }
                }
            }
        } else if(processMessage.eventType.equals(ProcessEvent.REQUEST_DISTANCE)) {
            if(distanceState.value() != Integer.MAX_VALUE && distanceState.value() != null) {
                collector.collect(ProcessMessage.forDistanceUpdate(processMessage.vertexEndpoint, processMessage.vertexId, distanceState.value()));
            }
        }

    }
}
