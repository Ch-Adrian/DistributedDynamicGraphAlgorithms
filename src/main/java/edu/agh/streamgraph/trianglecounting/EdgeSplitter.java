package edu.agh.streamgraph.trianglecounting;

import edu.agh.streamgraph.dynamicconnectedcomponents.ProcessMessage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
public class EdgeSplitter implements FlatMapFunction<Edge<Integer, NullValue>, Edge<Integer, NullValue>> {
//public class EdgeSplitter implements FlatMapFunction<QueryWrapper, QueryWrapper> {
    @Override
    public void flatMap(Edge<Integer, NullValue> edge, Collector<Edge<Integer, NullValue>> collector) throws Exception {
        collector.collect(edge);
        collector.collect(new Edge<Integer, NullValue> (edge.f1, edge.f0, NullValue.getInstance()));
    }

//    @Override
//    public void flatMap(QueryWrapper queryWrapper, Collector<QueryWrapper> collector) throws Exception {
//        if(queryWrapper.getState() == QueryState.COUNT_COEFFICIENT) {
//            collector.collect(queryWrapper);
//            return;
//        }
//        else if(queryWrapper.getState() != QueryState.ADD_EDGE) {
//            collector.collect(queryWrapper);
//            collector.collect(
//                    new QueryWrapper(
//                            new Edge<Integer, NullValue>(queryWrapper.getEdge().f1, queryWrapper.getEdge().f0, NullValue.getInstance()),
//                            queryWrapper.getState()));
//        }
//    }
}
