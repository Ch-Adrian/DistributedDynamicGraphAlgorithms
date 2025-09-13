package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;

public class MessageOperator implements FlatMapFunction<Edge<Integer, NullValue>, MessageWrapper> {

    private HashSet<Integer> processedVertices = new HashSet<>();
    private ArrayList<Integer> vertices = new ArrayList<>();

    @Override
    public void flatMap(Edge<Integer, NullValue> integerNullValueEdge, Collector<MessageWrapper> collector) throws Exception {
        MessageWrapper queryWrapper = new MessageWrapper(integerNullValueEdge, MessageState.ADD_EDGE);
        collector.collect(queryWrapper);

        if(!processedVertices.contains(integerNullValueEdge.f0)) {
            processedVertices.add(integerNullValueEdge.f0);
            vertices.add(integerNullValueEdge.f0);
        }

        if(!processedVertices.contains(integerNullValueEdge.f1)) {
            processedVertices.add(integerNullValueEdge.f1);
            vertices.add(integerNullValueEdge.f1);
        }

        if((Math.random() < 0.0001)) {
            Integer vertex1 = (int)Math.floor((Math.random() * vertices.size()));
            MessageWrapper queryWrapperCount = new MessageWrapper(
                    new Edge<Integer, NullValue>(vertex1, vertex1, NullValue.getInstance()),
                    MessageState.COUNT_COEFFICIENT);
            collector.collect(queryWrapperCount);
        }

    }
}
