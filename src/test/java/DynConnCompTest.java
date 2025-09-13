import edu.agh.streamgraph.dynamicconnectedcomponents.Main;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

public class DynConnCompTest {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

    @Test
    public void testConnectedComponents() {
        Collection<Edge<Integer, NullValue>> edges = new ArrayList<>();
        for(int i = 0; i<10; i++){
            edges.add(new Edge<Integer, NullValue> (i, i+1, NullValue.getInstance()));
        }
        System.out.println(edges.size());
        DataStream<Edge<Integer, NullValue>> initialEdges = env.fromCollection(edges).assignTimestampsAndWatermarks(
                WatermarkStrategy.forMonotonousTimestamps()
        );
        Main.getResultStream(initialEdges).map(processMessage -> processMessage.toString()).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) {
                System.out.println(value);
                return value;
            }
        });
    }
}
