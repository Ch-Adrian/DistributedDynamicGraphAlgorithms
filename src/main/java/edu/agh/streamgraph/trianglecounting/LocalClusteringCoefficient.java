package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LocalClusteringCoefficient extends KeyedProcessFunction<Integer, OutputMessageWrapper, String> {
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
    public void processElement(OutputMessageWrapper outputMessageWrapper,
                               KeyedProcessFunction<Integer, OutputMessageWrapper, String>.Context context,
                               Collector<String> collector) throws Exception {
        if(vertexId.value() == null){
            vertexId.update(context.getCurrentKey());
        }
        if(lccState.value() == null) {
            lccState.update(0.0);
        }
        if(triangleKeyedState.value() == null) {
            triangleKeyedState.update(new TriangleState(vertexId.value(), 0, 0));
        }
        if(triangleKeyedState.value().getNumOfNeighbors() < outputMessageWrapper.getTriangleState().getNumOfNeighbors()) {
            triangleKeyedState.update(outputMessageWrapper.getTriangleState());
        }
        if(triangleKeyedState.value().isValidForLCC()) {
            lccState.update((2.0 * triangleKeyedState.value().getAmtOfTriangles()) / (triangleKeyedState.value().getNumOfNeighbors() * (triangleKeyedState.value().getNumOfNeighbors() - 1)));
        }
        if (outputMessageWrapper.getState() == MessageState.COUNT_COEFFICIENT)
            collector.collect("Vertex: " + triangleKeyedState.value().getVertexID() + " has local clustering coefficient: " + lccState.value());
    }
}
