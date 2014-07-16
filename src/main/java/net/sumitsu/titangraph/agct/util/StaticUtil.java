package net.sumitsu.titangraph.agct.util;

public final class StaticUtil {

    public static boolean refEquals(final Object ref1, final Object ref2) {
        return (((ref1 == null) && (ref2 == null))
                ||
                ((ref1 != null) && (ref1.equals(ref2))));
    }
    
    public static String simplify(String str) {
        if (str != null) {
            str = str.trim();
            if ((str != null) && (str.length() <= 0)) {
                str = null;
            }
        }
        return str;
    }
    
    private StaticUtil() { }
}
