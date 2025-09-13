package edu.agh.streamgraph.trianglecounting;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.ArrayList;
import java.util.HashSet;

public class NeighborStorage {

    private static Integer nextStorageId = 0;
    private Integer storageId = 0;
    private final HashSet<Integer> neighbors;
    private final static MapStateDescriptor<Integer, NeighborStorage> descriptor = getMapStateDescriptor();
    public NeighborStorage(){
        neighbors = new HashSet<>();
        this.storageId = nextStorageId++;
    }

    public void addNeighbor(Integer neighbor){
        neighbors.add(neighbor);
    }

    public void removeNeighbor(Integer neighbor){
        neighbors.remove(neighbor);
    }

    public Integer getNeighborAmt(){
        return neighbors.size();
    }

    public Integer getStorageId() {
        return storageId;
    }

    public static MapStateDescriptor<Integer, NeighborStorage> getDescriptor() {
        return descriptor;
    }

    public boolean containsNeighbor(Integer neighbor){
        return neighbors.contains(neighbor);
    }

    public ArrayList<Integer> getCommonVertices(NeighborStorage other){
        ArrayList<Integer> common = new ArrayList<>();
        for(Integer neighbor : neighbors){
            if(other.neighbors.contains(neighbor)){
                common.add(neighbor);
            }
        }
        return common;
    }

    private static MapStateDescriptor<Integer, NeighborStorage> getMapStateDescriptor(){
        return new MapStateDescriptor<>(
                "NeighborStorage",
                TypeInformation.of(Integer.class),
                TypeInformation.of(new TypeHint<NeighborStorage>() {}));
    }


}
