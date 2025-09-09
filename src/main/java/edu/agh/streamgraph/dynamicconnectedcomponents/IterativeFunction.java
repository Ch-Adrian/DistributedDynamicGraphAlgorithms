package edu.agh.streamgraph.dynamicconnectedcomponents;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;

public class IterativeFunction extends KeyedProcessFunction<Long, ProcessMessage, ProcessMessage> {

    private ValueState<Long> vertexId;
    private ValueState<Long> labelState;
    private MapState<Long, Long> neighborsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        labelState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("label", Long.class)
        );
        neighborsState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("neighbors", Long.class, Long.class)
        );
        vertexId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("vertexId", Long.class)
        );;
    }

    @Override
    public void processElement(ProcessMessage processMessage, KeyedProcessFunction<Long, ProcessMessage, ProcessMessage>.Context context, Collector<ProcessMessage> collector) throws Exception {
        System.out.println("Processing message for vertex " + context.getCurrentKey() + ": " + processMessage);
        if(vertexId.value() == null) vertexId.update(context.getCurrentKey());
        if(labelState.value() == null) labelState.update(vertexId.value());

        if(processMessage.eventType.equals(ProcessEvent.EDGE_INCOMING)){
            Edge<Long, NullValue>  edge = processMessage.edge;
            boolean isChange = edge.f1 < labelState.value();
            if(isChange){
                labelState.update(edge.f1);
                for(Long neighbor : neighborsState.keys()){
                    collector.collect(ProcessMessage.forInternalMessage(new InternalMessage(neighbor, labelState.value())));
                }
                if(!neighborsState.contains(edge.f1)){
                    neighborsState.put(edge.f1, 1L);
                }
            }
            collector.collect(ProcessMessage.forEdgeOutgoing(edge, new InternalMessage(vertexId.value(), labelState.value())));
        }
        else if(processMessage.eventType.equals(ProcessEvent.INTERNAL_MESSAGE)) {
            InternalMessage internalMessage = processMessage.internalMessage;
            if (internalMessage.componentId < labelState.value()) {
                labelState.update(internalMessage.componentId);
                for (Long neighbor : neighborsState.keys()) {
                    collector.collect(ProcessMessage.forInternalMessage(new InternalMessage(neighbor, labelState.value())));
                }
            }
            collector.collect(ProcessMessage.forEdgeOutgoing(null, new InternalMessage(vertexId.value(), labelState.value())));
        }

    }
}
