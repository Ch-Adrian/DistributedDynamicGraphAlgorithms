package edu.agh.streamgraph.dynamicconnectedcomponents;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

public class Main {

    static int parallelism = 4;

    public static void main(String[] args) throws Exception {
//        testIteration();
        long startTime = System.currentTimeMillis();
        testIter();
        long endTime = System.currentTimeMillis();
        System.out.println("Execution time: " + (endTime - startTime) + " ms");
    }

    public static void testIteration() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(parallelism);

        DataStream<Edge<Integer, NullValue> > initialEdges = env.fromElements(
                new Edge<Integer, NullValue> (1, 2, NullValue.getInstance()),
                new Edge<Integer, NullValue> (2, 3, NullValue.getInstance()),
                new Edge<Integer, NullValue> (4, 5, NullValue.getInstance())
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge<Integer, NullValue> >forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis())
                        .withIdleness(Duration.ofSeconds(5))
        );

        getResultStream(initialEdges).keyBy(processMessage -> 0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ConnectivityCheck());

        env.execute();
    }

    public static void testIter() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.disableOperatorChaining();

        Collection<Edge<Integer, NullValue>> edges = new ArrayList<>();
        for(int i = 0; i<1000000; i++){
            Integer random1 = (int) (Math.random() * 10000000);
            Integer random2 = (int) (Math.random() * 10000000);
            edges.add(new Edge<Integer, NullValue> (random1, random2, NullValue.getInstance()));
//
//            edges.add(new Edge<Integer, NullValue> (i, i+1, NullValue.getInstance()));
        }
        System.out.println(edges.size());
        DataStream<Edge<Integer, NullValue>> initialEdges = env.fromCollection(edges).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        );
        Main.getResultStream(initialEdges);
//        .map(processMessage -> processMessage.toString()).map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) {
//                System.out.println(value);
//                return value;
//            }
//        });

        env.execute();
    }

    public static DataStream<ProcessMessage> getResultStream(DataStream<Edge<Integer, NullValue>> graphEdgeStream) {
        IterativeStream<ProcessMessage> internalMessages = graphEdgeStream
                .flatMap(new EdgeSplitter())
                .returns(ProcessMessage.class).setParallelism(parallelism)
                .iterate(5000L);

        DataStream<ProcessMessage> mainProcess = internalMessages
                .keyBy(processMessage -> processMessage.vertexId)
                .process(new IterativeFunction()).disableChaining().setParallelism(parallelism);


        DataStream<ProcessMessage> feedback = mainProcess
                .filter(processMessage -> !processMessage.eventType.equals(ProcessEvent.EDGE_OUTGOING)).setParallelism(parallelism);

        DataStream<ProcessMessage> toSink = mainProcess
                .filter(processMessage -> processMessage.eventType.equals(ProcessEvent.EDGE_OUTGOING)).setParallelism(parallelism);

        internalMessages.closeWith(feedback);

        return toSink;
    }

}