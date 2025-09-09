package edu.agh.streamgraph.dynamicconnectedcomponents;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;

public class EdgeSplitter implements FlatMapFunction<Edge<Long, NullValue> , ProcessMessage> {
    @Override
    public void flatMap(Edge<Long, NullValue>  edge, Collector<ProcessMessage> collector) throws Exception {
        collector.collect(ProcessMessage.forEdgeIncoming(edge));
        collector.collect(ProcessMessage.forEdgeIncoming(new Edge<Long, NullValue> (edge.f1, edge.f0, NullValue.getInstance())));
    }
}
