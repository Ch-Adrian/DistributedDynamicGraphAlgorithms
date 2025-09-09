package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeWrapper implements FlatMapFunction<Edge<Integer, NullValue> , ProcessMessage> {

    @Override
    public void flatMap(Edge<Integer, NullValue> edge, Collector<ProcessMessage> collector) throws Exception {
        collector.collect(ProcessMessage.forVertexAddition(edge));
        collector.collect(ProcessMessage.forVertexAddition(new Edge<Integer, NullValue> (edge.f1, edge.f0, NullValue.getInstance())));
    }
}
