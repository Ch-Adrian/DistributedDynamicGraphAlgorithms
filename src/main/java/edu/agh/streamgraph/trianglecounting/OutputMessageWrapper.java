package edu.agh.streamgraph.trianglecounting;

public class OutputMessageWrapper {
    private TriangleState triangleState;
    private MessageState state;

    public OutputMessageWrapper(TriangleState triangleState, MessageState state) {
        this.triangleState = triangleState;
        this.state = state;
    }

    public TriangleState getTriangleState() {
        return triangleState;
    }

    public MessageState getState() {
        return state;
    }

    public void setState(MessageState state) {
        this.state = state;
    }

    public void setTriangleState(TriangleState triangleState) {
        this.triangleState = triangleState;
    }
}
