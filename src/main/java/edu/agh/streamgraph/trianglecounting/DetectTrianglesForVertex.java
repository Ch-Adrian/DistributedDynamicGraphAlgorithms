package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class DetectTrianglesForVertex extends KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, Edge<Integer, NullValue>, TriangleState> {

    private ValueState<Integer> amtOfTriangles;
    private ValueState<Integer> vertexId;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>(
                        "amtOfTriangles", // the state name
                        Integer.class); // type information
        amtOfTriangles = getRuntimeContext().getState(descriptor);
        vertexId = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                "vertexId",
                Integer.class
        ));
    }

    @Override
    public void processElement(Edge<Integer, NullValue> edge,
                               KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, Edge<Integer, NullValue>, TriangleState>.ReadOnlyContext readOnlyContext,
                               Collector<TriangleState> collector) throws Exception {
        if(vertexId.value() == null){
            vertexId.update(readOnlyContext.getCurrentKey());
        }
//        System.out.println("Processing edge: " + edge + " for vertex: " + readOnlyContext.getCurrentKey());
        ReadOnlyBroadcastState<Integer, NeighborStorage> broadcastState = readOnlyContext.getBroadcastState(NeighborStorage.getDescriptor());
        NeighborStorage neighborStorage0 = broadcastState.get(edge.f0);
        if(neighborStorage0 == null) neighborStorage0 = new NeighborStorage();
        NeighborStorage neighborStorage1 = broadcastState.get(edge.f1);
        if(neighborStorage1 == null) neighborStorage1 = new NeighborStorage();

        ArrayList<Integer> commonVertices = neighborStorage0.getCommonVertices(neighborStorage1);
//        System.out.println("Common vertices for edge " + edge + ": " + commonVertices.size());
        if(!commonVertices.isEmpty()){
            collector.collect(new TriangleState(edge.f0, commonVertices.size(), neighborStorage0.getNeighborAmt()));
            collector.collect(new TriangleState(edge.f1, commonVertices.size(), neighborStorage1.getNeighborAmt()));
            for(Integer commonVertex : commonVertices){
                NeighborStorage neighborStorage = broadcastState.get(commonVertex);
                Integer neighborAmt = 0;
                if (neighborStorage != null) neighborAmt = neighborStorage.getNeighborAmt();
                collector.collect(new TriangleState(commonVertex, commonVertices.size(), neighborAmt));
            }
        }

    }

    @Override
    public void processBroadcastElement(Edge<Integer, NullValue> edge,
                                        KeyedBroadcastProcessFunction<Integer, Edge<Integer, NullValue>, Edge<Integer, NullValue>, TriangleState>.Context context,
                                        Collector<TriangleState> collector) throws Exception {
//        System.out.println("Broadcasting edge: " + edge);
        BroadcastState<Integer, NeighborStorage> broadcastState = context.getBroadcastState(NeighborStorage.getDescriptor());

        NeighborStorage neighborStorage0 = broadcastState.get(edge.f0);
        if(neighborStorage0 == null) neighborStorage0 = new NeighborStorage();
        if(!neighborStorage0.containsNeighbor(edge.f1))
            neighborStorage0.addNeighbor(edge.f1);
        broadcastState.put(edge.f0, neighborStorage0);

        NeighborStorage neighborStorage1 = broadcastState.get(edge.f1);
        if(neighborStorage1 == null) neighborStorage1 = new NeighborStorage();
        if(!neighborStorage1.containsNeighbor(edge.f0))
            neighborStorage1.addNeighbor(edge.f0);
        broadcastState.put(edge.f1, neighborStorage1);

    }
}
