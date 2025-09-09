package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeWrapper implements FlatMapFunction<Edge<Long, NullValue> , ProcessMessage> {

    @Override
    public void flatMap(Edge<Long, NullValue> edge, Collector<ProcessMessage> collector) throws Exception {
        collector.collect(ProcessMessage.forVertexAddition(edge));
        collector.collect(ProcessMessage.forVertexAddition(new Edge<Long, NullValue> (edge.f1, edge.f0, NullValue.getInstance())));
    }
}
