package edu.agh.streamgraph.dynsssp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        testIteration();
    }

    public static void testIteration() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);

        DataStream<Edge> initialEdges = env.fromElements(
                new Edge(1L, 2L),
                new Edge(2L, 3L),
                new Edge(3L, 4L),
                new Edge(2L, 5L),
                new Edge(5L, 6L),
                new Edge(6L, 7L),
                new Edge(7L, 8L),
                new Edge(8L, 4L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge>forBoundedOutOfOrderness(Duration.ofSeconds(1))
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