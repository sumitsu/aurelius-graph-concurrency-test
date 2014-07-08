package net.sumitsu.titangraph.agct;

public class VertexPlaceKeeper {

    private int primaryIndex;
    private final int primariesTotal;
    private int remainingSecondariesOnCurrentPrimary;
    private final int secondariesPerPrimary;
    
    public synchronized RecordIdentifier dequeue() {
        RecordIdentifier ri = null;
        if (this.primaryIndex < this.primariesTotal) {
            ri = new RecordIdentifier(
                    this.primaryIndex,
                    (this.secondariesPerPrimary - this.remainingSecondariesOnCurrentPrimary));
            this.remainingSecondariesOnCurrentPrimary--;
            if (this.remainingSecondariesOnCurrentPrimary <= 0) {
                this.primaryIndex++;
                this.remainingSecondariesOnCurrentPrimary = this.secondariesPerPrimary;
            }
        }
        return ri;
    }
    
    public VertexPlaceKeeper(final int primariesTotal, final int secondariesPerPrimary) {
        this.primariesTotal = primariesTotal;
        this.secondariesPerPrimary = secondariesPerPrimary;
        this.primaryIndex = 0;
        this.remainingSecondariesOnCurrentPrimary = secondariesPerPrimary;
    }

}
