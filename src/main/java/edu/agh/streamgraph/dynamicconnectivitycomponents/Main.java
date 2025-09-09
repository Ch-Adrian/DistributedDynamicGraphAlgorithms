package edu.agh.streamgraph.dynamicconnectivitycomponents;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        testIteration3();
    }

    public static void testIteration3() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);

        DataStream<Edge> initialEdges = env.fromElements(
                new Edge(1L, 2L),
                new Edge(2L, 3L),
                new Edge(4L, 5L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis())
                        .withIdleness(Duration.ofSeconds(5))
        );

        IterativeStream<ProcessMessage> internalMessages = initialEdges
                .flatMap(new EdgeSplitter())
                .returns(ProcessMessage.class)
                .iterate(5000L);

        DataStream<ProcessMessage> mainProcess = internalMessages
                .keyBy(processMessage -> processMessage.vertexId)
                .process(new IterativeFunction());


        DataStream<ProcessMessage> feedback = mainProcess
                .filter(processMessage -> !processMessage.eventType.equals(ProcessEvent.EDGE_OUTGOING));

        DataStream<ProcessMessage> toSink = mainProcess
                .filter(processMessage -> processMessage.eventType.equals(ProcessEvent.EDGE_OUTGOING));

        internalMessages.closeWith(feedback);

        toSink.keyBy(processMessage -> 0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ConnectivityCheck());


        env.execute();
    }

}