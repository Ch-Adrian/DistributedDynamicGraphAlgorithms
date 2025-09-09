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

        DataStream<Edge<Integer, NullValue> > initialEdges = env.fromElements(
                new Edge<Integer, NullValue> (1, 2, NullValue.getInstance()),
                new Edge<Integer, NullValue> (2, 3, NullValue.getInstance()),
                new Edge<Integer, NullValue> (3, 4, NullValue.getInstance()),
                new Edge<Integer, NullValue> (2, 5, NullValue.getInstance()),
                new Edge<Integer, NullValue> (5, 6, NullValue.getInstance()),
                new Edge<Integer, NullValue> (6, 7, NullValue.getInstance()),
                new Edge<Integer, NullValue> (7, 8, NullValue.getInstance()),
                new Edge<Integer, NullValue> (8, 4, NullValue.getInstance())
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge<Integer, NullValue>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
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