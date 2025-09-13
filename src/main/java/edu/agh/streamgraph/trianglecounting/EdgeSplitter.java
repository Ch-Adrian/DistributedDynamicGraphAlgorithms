package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class EdgeSplitter implements FlatMapFunction<MessageWrapper, MessageWrapper> {

    @Override
    public void flatMap(MessageWrapper queryWrapper, Collector<MessageWrapper> collector) throws Exception {
        if(queryWrapper.getState() == MessageState.COUNT_COEFFICIENT) {
            collector.collect(queryWrapper);
        }
        else if(queryWrapper.getState() == MessageState.ADD_EDGE) {
            collector.collect(queryWrapper);
            collector.collect(
                    new MessageWrapper(
                            new Edge<Integer, NullValue>(queryWrapper.getEdge().f1, queryWrapper.getEdge().f0, NullValue.getInstance()),
                            queryWrapper.getState()));
        }
    }

}
