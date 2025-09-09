package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;


import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        testIteration();
    }

    public static void testIteration() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);

        DataStream<Edge<Long, NullValue> > initialEdges = env.fromElements(
                new Edge<Long, NullValue> (1L, 2L, NullValue.getInstance()),
                new Edge<Long, NullValue> (2L, 3L, NullValue.getInstance()),
                new Edge<Long, NullValue> (3L, 4L, NullValue.getInstance()),
                new Edge<Long, NullValue> (2L, 5L, NullValue.getInstance()),
                new Edge<Long, NullValue> (5L, 6L, NullValue.getInstance()),
                new Edge<Long, NullValue> (6L, 7L, NullValue.getInstance()),
                new Edge<Long, NullValue> (7L, 8L, NullValue.getInstance()),
                new Edge<Long, NullValue> (8L, 4L, NullValue.getInstance())
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge<Long, NullValue>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis())
                        .withIdleness(Duration.ofSeconds(5))
        );

        IterativeStream<ProcessMessage> internalMessages = initialEdges
                .flatMap(new EdgeWrapper())
                .returns(ProcessMessage.class)
                .iterate(5000L);

        DataStream<ProcessMessage> mainProcess = internalMessages
                .keyBy(processMessage -> processMessage.vertexId)
                .process(new DistanceProcessor());

        DataStream<ProcessMessage> feedback = mainProcess
                .filter(processMessage -> !processMessage.eventType.equals(ProcessEvent.VERTEX_OUTGOING));

        DataStream<ProcessMessage> toSink = mainProcess
                .filter(processMessage -> processMessage.eventType.equals(ProcessEvent.VERTEX_OUTGOING));

        internalMessages.closeWith(feedback);

        toSink.print();

        env.execute();
    }
}