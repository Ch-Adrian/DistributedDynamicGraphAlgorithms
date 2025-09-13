package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class QueryOperator implements FlatMapFunction<Edge<Integer, NullValue>, QueryWrapper> {

    private HashSet<Integer> vertices = new HashSet<>();

    @Override
    public void flatMap(Edge<Integer, NullValue> integerNullValueEdge, Collector<QueryWrapper> collector) throws Exception {
        QueryWrapper queryWrapper = new QueryWrapper(integerNullValueEdge, QueryState.ADD_EDGE);
        collector.collect(queryWrapper);

        if(!vertices.contains(integerNullValueEdge.f0)) {
            vertices.add(integerNullValueEdge.f0);
        }
        if(!vertices.contains(integerNullValueEdge.f1)) {
            vertices.add(integerNullValueEdge.f1);
        }

        if((Math.random() < 0.0001)) {
            QueryWrapper queryWrapperCount = new QueryWrapper(integerNullValueEdge, QueryState.COUNT_COEFFICIENT);
            collector.collect(queryWrapperCount);
        }

    }
}
