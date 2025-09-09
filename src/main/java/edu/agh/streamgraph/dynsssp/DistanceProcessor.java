package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class DistanceProcessor extends KeyedProcessFunction<Long, ProcessMessage, ProcessMessage> {

    private ValueState<Long> vertexId;
    private ValueState<Long> distanceState;
    private MapState<Long, Long> neighborsIds;
    private Long soureceId = 1L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        distanceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("label", Long.class)
        );
        neighborsIds = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("neighbors", Long.class, Long.class)
        );
        vertexId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("vertexId", Long.class)
        );
    }

    @Override
    public void processElement(ProcessMessage processMessage,
                               KeyedProcessFunction<Long, ProcessMessage, ProcessMessage>.Context context,
                               Collector<ProcessMessage> collector) throws Exception {
        if(vertexId.value() == null) vertexId.update(context.getCurrentKey());
        if(distanceState.value() == null) distanceState.update(Long.MAX_VALUE);

        if(processMessage.eventType.equals(ProcessEvent.ADD_VERTEX)){
            System.out.println("Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
            Long vertexEnding = processMessage.vertexEndpoint;
            if(!neighborsIds.contains(vertexEnding)){
                neighborsIds.put(vertexEnding, Long.MAX_VALUE);
            }
            if(Objects.equals(this.soureceId, processMessage.vertexId)){
                distanceState.update(0L);
                collector.collect(ProcessMessage.forVertexOutgoing(vertexId.value(), vertexEnding, 0L));
                for(Long neighbor : neighborsIds.keys()){
                    collector.collect(ProcessMessage.forDistanceUpdate(neighbor, vertexId.value(), 0L));
                }
            } else {
                if(distanceState.value() != Long.MAX_VALUE){
                    collector.collect(ProcessMessage.forDistanceUpdate(vertexEnding, vertexId.value(), distanceState.value()));
                } else {
                    collector.collect(ProcessMessage.forDistanceRequest(vertexEnding, vertexId.value()) );
                }
            }
        }
        else if(processMessage.eventType.equals(ProcessEvent.UPDATE_DISTANCE)) {
            System.out.println("Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
            if(processMessage.distance == Long.MAX_VALUE)
                return;
            Long newDistance = processMessage.distance + 1L;
            if (newDistance < distanceState.value()) {
                distanceState.update(newDistance);
                collector.collect(ProcessMessage.forVertexOutgoing(vertexId.value(), processMessage.vertexEndpoint, newDistance));
                for (Long neighbor : neighborsIds.keys()) {
                    collector.collect(ProcessMessage.forDistanceUpdate(neighbor, vertexId.value(), distanceState.value()));
                }
            }
        } else if(processMessage.eventType.equals(ProcessEvent.REQUEST_DISTANCE)) {
            System.out.println("Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
            collector.collect(ProcessMessage.forDistanceUpdate(processMessage.vertexEndpoint, processMessage.vertexId, distanceState.value()));
        }

    }
}
