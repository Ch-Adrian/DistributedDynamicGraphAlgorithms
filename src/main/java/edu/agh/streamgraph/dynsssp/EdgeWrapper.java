package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class EdgeWrapper implements FlatMapFunction<Edge, ProcessMessage> {

    @Override
    public void flatMap(Edge edge, Collector<ProcessMessage> collector) throws Exception {
        collector.collect(ProcessMessage.forVertexAddition(edge));
        collector.collect(ProcessMessage.forVertexAddition(new Edge(edge.target, edge.source)));
    }
}
