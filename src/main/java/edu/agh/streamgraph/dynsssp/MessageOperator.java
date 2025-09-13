package edu.agh.streamgraph.dynsssp;

import edu.agh.streamgraph.trianglecounting.MessageState;
import edu.agh.streamgraph.trianglecounting.MessageWrapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;

public class MessageOperator implements FlatMapFunction<Edge<Integer, NullValue>, ProcessMessage> {

    private HashSet<Integer> processedVertices = new HashSet<>();
    private ArrayList<Integer> vertices = new ArrayList<>();

    @Override
    public void flatMap(Edge<Integer, NullValue> integerNullValueEdge, Collector<ProcessMessage> collector) throws Exception {
        ProcessMessage processMessage = new ProcessMessage(ProcessEvent.ADD_VERTEX, integerNullValueEdge.f0, integerNullValueEdge.f1, null);
        collector.collect(processMessage);

        if(!processedVertices.contains(integerNullValueEdge.f0)) {
            processedVertices.add(integerNullValueEdge.f0);
            vertices.add(integerNullValueEdge.f0);
        }

        if(!processedVertices.contains(integerNullValueEdge.f1)) {
            processedVertices.add(integerNullValueEdge.f1);
            vertices.add(integerNullValueEdge.f1);
        }

        if((Math.random() < 0.003)) {
            Integer vertex1 = (int)Math.floor((Math.random() * vertices.size()));
            ProcessMessage queryWrapperCount = new ProcessMessage(ProcessEvent.QUERY_DISTANCE, vertices.get(vertex1), null, null);
            collector.collect(queryWrapperCount);
        }

    }

}
