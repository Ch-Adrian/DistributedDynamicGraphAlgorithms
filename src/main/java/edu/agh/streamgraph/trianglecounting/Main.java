package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

public class Main {
    public static void main(String[] args) throws Exception {
//        testIteration();
//        testBruteForce();
        testTriangles();
    }

    public static void testIteration() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);

        DataStream<Edge<Integer, NullValue>> initialEdges = env.fromElements(
                new Edge<Integer, NullValue>(1, 2, NullValue.getInstance()),
                new Edge<Integer, NullValue>(2, 3, NullValue.getInstance()),
                new Edge<Integer, NullValue>(3, 1, NullValue.getInstance()),
                new Edge<Integer, NullValue>(2, 5, NullValue.getInstance()),
                new Edge<Integer, NullValue>(5, 6, NullValue.getInstance()),
                new Edge<Integer, NullValue>(6, 7, NullValue.getInstance()),
                new Edge<Integer, NullValue>(7, 8, NullValue.getInstance()),
                new Edge<Integer, NullValue>(8, 4, NullValue.getInstance())
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Edge<Integer, NullValue>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis())
                        .withIdleness(Duration.ofSeconds(5))
        );

        getResultStream(initialEdges);

        env.execute();

    }


    public static void testBruteForce() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
        getResultStream(initialEdges);
//        .map(processMessage -> processMessage.toString()).map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) {
//                System.out.println(value);
//                return value;
//            }
//        });

        env.execute();
    }

    public static void testTriangles() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.disableOperatorChaining();

        ArrayList<Edge<Integer, NullValue>> edges = new ArrayList<>();
        for(int i = 0; i<10000; i++){
            Integer random1 = (int) (Math.random() * 100000);
            Integer random2 = (int) (Math.random() * 100000);
            edges.add(new Edge<Integer, NullValue> (random1, random2, NullValue.getInstance()));
        }
        System.out.println(edges.size());
        int size = edges.size();

        for(int i = 0; i< size; i++){
            Edge<Integer, NullValue> edge = edges.get(i);
            Integer random3 = (int) (Math.random() * 100000);
            edges.add(new Edge<Integer, NullValue> (edge.f1, random3, NullValue.getInstance()));
            edges.add(new Edge<Integer, NullValue> (edge.f0, random3, NullValue.getInstance()));
        }

        ArrayList<Edge<Integer, NullValue>> edges2 = new ArrayList<>();
        edges2.addAll(edges);
        edges2.addAll(edges);
        edges2.addAll(edges);
        System.out.println("Edges size: "+edges.size());
        System.out.println("Edges2 size: "+edges2.size());

        DataStream<Edge<Integer, NullValue>> initialEdges = env.fromCollection(edges2).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        );
        getResultStream(initialEdges);

        env.execute();
    }


    public static SingleOutputStreamOperator<String> getResultStream(DataStream<Edge<Integer, NullValue>> graphEdgeStream) {
        BroadcastStream<Edge<Integer, NullValue>> broadcastStream = graphEdgeStream
                .flatMap(new QueryOperator())
                .flatMap(new EdgeSplitter())
                .broadcast(NeighborStorage.getDescriptor());

        BroadcastConnectedStream<Edge<Integer, NullValue>, Edge<Integer, NullValue>> broadcastConnectedStream =
                graphEdgeStream
                        .keyBy(edge -> edge.f0)
                        .connect(broadcastStream);

        SingleOutputStreamOperator<TriangleState> singleOutputStreamOperator = broadcastConnectedStream
                .process(new DetectTrianglesForVertex());

        SingleOutputStreamOperator<String> localClusterCoefficient = singleOutputStreamOperator
                .keyBy(TriangleState::getVertexID)
                .reduce((TriangleState t1, TriangleState t2) -> {
                    if(t1.getNumOfNeighbors() > t2.getNumOfNeighbors()){
                        return t1;
                    } else {
                        return t2;
                    }
                })
                .keyBy(TriangleState::getVertexID)
                .process(new LocalClusteringCoefficient());

        localClusterCoefficient.map(new RichMapFunction<String, String>() {

            private Integer countTriangles;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                countTriangles = 0;
            }

            @Override
            public String map(String value) throws Exception {
                countTriangles = countTriangles + 1;
                System.out.println("Processed triangles: " + countTriangles);
                return value;
            }
        }).setParallelism(1);

        return localClusterCoefficient;
    }

}
