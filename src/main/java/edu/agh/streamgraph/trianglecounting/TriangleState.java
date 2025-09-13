package edu.agh.streamgraph.trianglecounting;

public class TriangleState {
    private final Integer vertexID;
    private final Integer amtOfTriangles;
    private final Integer numOfNeighbors;
    public TriangleState(Integer vertexID, Integer amtOfTriangles, Integer numOfNeighbors) {
        this.vertexID = vertexID;
        this.amtOfTriangles = amtOfTriangles;
        this.numOfNeighbors = numOfNeighbors;
    }

    public Integer getVertexID() {
        return vertexID;
    }

    public Integer getAmtOfTriangles() {
        return amtOfTriangles;
    }

    public Integer getNumOfNeighbors() {
        return numOfNeighbors;
    }

    public boolean isValidForLCC(){
        return numOfNeighbors > 1;
    }

}
