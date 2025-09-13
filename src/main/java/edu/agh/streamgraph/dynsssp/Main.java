package edu.agh.streamgraph.dynsssp;

import edu.agh.streamgraph.dynamicconnectedcomponents.IterativeFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
//        testIteration();
        testBruteForce();
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

        getResultStream(initialEdges).print();

        env.execute();
    }

    public static void testBruteForce() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Collection<Edge<Integer, NullValue>> edges = new ArrayList<>();
        for(int i = 0; i<100000; i++){
            Integer random1 = (int) (Math.random() * 10000000);
            Integer random2 = (int) (Math.random() * 10000000);
            edges.add(new Edge<Integer, NullValue> (i, i+1, NullValue.getInstance()));
        }
        System.out.println(edges.size());

        DataStream<Edge<Integer, NullValue>> initialEdges = env.fromCollection(edges).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        );

        getResultStream(initialEdges).print();

        env.execute();
    }



    public static DataStream<ProcessMessage> getResultStream(DataStream<Edge<Integer, NullValue>> graphEdgeStream) {
        IterativeStream<ProcessMessage> internalMessages = graphEdgeStream
                .flatMap(new MessageOperator())
                .flatMap(new EdgeSplitter())
                .returns(ProcessMessage.class)
                .iterate(3000L);

        DataStream<ProcessMessage> mainProcess = internalMessages
                .keyBy(processMessage -> processMessage.vertexId)
                .process(new DistanceProcessor());

        DataStream<ProcessMessage> feedback = mainProcess
                .filter(processMessage -> !processMessage.eventType.equals(ProcessEvent.VERTEX_OUTGOING));

        DataStream<ProcessMessage> toSink = mainProcess
                .filter(processMessage -> processMessage.eventType.equals(ProcessEvent.VERTEX_OUTGOING));

        internalMessages.closeWith(feedback);

        return toSink;
    }

}