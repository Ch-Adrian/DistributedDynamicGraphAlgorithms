package edu.agh.streamgraph.dynamicconnectivitycomponents;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class EdgeSplitter implements FlatMapFunction<Edge, ProcessMessage> {
    @Override
    public void flatMap(Edge edge, Collector<ProcessMessage> collector) throws Exception {
        collector.collect(ProcessMessage.forEdgeIncoming(edge));
        collector.collect(ProcessMessage.forEdgeIncoming(new Edge(edge.target, edge.source)));
    }
}
