package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeSplitter implements FlatMapFunction<ProcessMessage, ProcessMessage> {

    @Override
    public void flatMap(ProcessMessage message, Collector<ProcessMessage> collector) throws Exception {
        if(message.eventType == ProcessEvent.ADD_VERTEX) {
            collector.collect(message);
            collector.collect(ProcessMessage.forVertexAddition(new Edge<> (message.vertexEndpoint, message.vertexId, NullValue.getInstance())));
        } else if(message.eventType == ProcessEvent.QUERY_DISTANCE) {
            collector.collect(message);
        }
    }

}
