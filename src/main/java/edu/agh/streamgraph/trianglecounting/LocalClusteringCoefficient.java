package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LocalClusteringCoefficient extends KeyedProcessFunction<Integer, TriangleState, String> {
    private ValueState<Integer> vertexId;
    private ValueState<TriangleState> triangleKeyedState;
    private ValueState<Double> lccState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        vertexId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("vertexId", Integer.class)
        );
        triangleKeyedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("triangleState", TriangleState.class)
        );
        lccState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lccState", Double.class)
        );

    }

    @Override
    public void processElement(TriangleState triangleState,
                               KeyedProcessFunction<Integer, TriangleState, String>.Context context,
                               Collector<String> collector) throws Exception {
        if(vertexId.value() == null){
            vertexId.update(context.getCurrentKey());
        }
        triangleKeyedState.update(triangleState);
        if(triangleKeyedState.value().isValidForLCC()) {
            lccState.update((2.0 * triangleKeyedState.value().getAmtOfTriangles()) / (triangleKeyedState.value().getNumOfNeighbors() * (triangleKeyedState.value().getNumOfNeighbors() - 1)));
        }
        collector.collect("Vertex: " + triangleState.getVertexID() + " has local clustering coefficient: " + lccState.value());
    }
}
