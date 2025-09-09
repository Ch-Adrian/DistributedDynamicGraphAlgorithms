package edu.agh.streamgraph.dynamicconnectivitycomponents;

import edu.agh.streamgraph.dynamicconnectivitycomponents.algorithmConnectivityIncremental.Graph;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

public class ConnectivityCheck extends ProcessWindowFunction<ProcessMessage, Object, Integer, TimeWindow> {

    private Graph graph = new Graph();
    private Map<Integer, Integer> vertexToComponent = new java.util.HashMap<>();
    private Random rand = new Random();

    @Override
    public void process(Integer integer, ProcessWindowFunction<ProcessMessage, Object, Integer, TimeWindow>.Context context, Iterable<ProcessMessage> iterable, Collector<Object> collector) throws Exception {
        System.out.println("Window:");
        for(ProcessMessage pm : iterable){
            if(pm.edge != null){
                vertexToComponent.put(Math.toIntExact(pm.edge.source),
                        Math.toIntExact(pm.internalMessage.componentId));
                graph.addNonDirectedEdge(Math.toIntExact(pm.edge.source), Math.toIntExact(pm.edge.target));
                System.out.println("Added edge: " + pm.edge);
            }
        }

        ArrayList<Integer> vertices = new ArrayList<>(vertexToComponent.keySet());
        for(Map.Entry<Integer, Integer> entry : vertexToComponent.entrySet()){
            vertices.add(entry.getKey());
        }

        for(int i = 0; i<3; i++){
            int randomIntBegin = rand.nextInt(vertices.size());
            int randomIntEnd = rand.nextInt(vertices.size());
            int begin = vertices.get(randomIntBegin);
            int end = vertices.get(randomIntEnd);
            if(graph.isConnected(begin, end) == vertexToComponent.get(begin).equals(vertexToComponent.get(end))){
                System.out.println("Vertices " + begin + " and " + end + " are valid");
                collector.collect("true");
            } else {
                System.out.println("Vertices " + begin + " and " + end + " are NOT valid");
                collector.collect("false");
            }
        }
        System.out.println("End of window");
    }
}
