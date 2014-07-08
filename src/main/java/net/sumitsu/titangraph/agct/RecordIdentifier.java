package net.sumitsu.titangraph.agct;

public class RecordIdentifier {

    private final int primary;
    private final int secondary;
    private final String strRep;
    
    public int getPrimary() {
        return this.primary;
    }
    public int getSecondary() {
        return this.secondary;
    }
    
    @Override
    public int hashCode() {
        return this.primary ^ this.secondary;
    }
    @Override
    public boolean equals(final Object otherObj) {
        final RecordIdentifier otherRI;
        if (otherObj instanceof RecordIdentifier) {
            otherRI = (RecordIdentifier) otherObj;
            return ((this.primary == otherRI.primary)
                    &&
                    (this.secondary == otherRI.secondary));
        }
        return false;
    }
    @Override
    public String toString() {
        return this.strRep;
    }
    
    public RecordIdentifier(final int primary, final int secondary) {
        final StringBuilder strGen;
        
        this.primary = primary;
        this.secondary = secondary;
        
        strGen = new StringBuilder();
        strGen.append("RecordIdentifier(primary=");
        strGen.append(primary);
        strGen.append(",secondary=");
        strGen.append(secondary);
        strGen.append(")");
        this.strRep = strGen.toString();
    }
}
