package edu.agh.streamgraph.dynamicconnectedcomponents;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.time.Duration;

public class Main {
    public static void main(String[] args) throws Exception {
        testIteration3();
    }

    public static void testIteration3() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);

        DataStream<Edge<Long, NullValue> > initialEdges = env.fromElements(
                new Edge<Long, NullValue> (1L, 2L, NullValue.getInstance()),
                new Edge<Long, NullValue> (2L, 3L, NullValue.getInstance()),
                new Edge<Long, NullValue> (4L, 5L, NullValue.getInstance())
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge<Long, NullValue> >forBoundedOutOfOrderness(Duration.ofSeconds(1))
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