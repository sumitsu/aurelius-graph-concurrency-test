package net.sumitsu.titangraph.agct;

import java.util.Scanner;

public class GraphLoaderCommand {
    
    public static final String SYSPROP_COUNT_VERTEX_PRIMARY   = "COUNT_VERTEX_PRIMARY";
    public static final String SYSPROP_COUNT_VERTEX_SECONDARY = "COUNT_VERTEX_SECONDARY";
    public static final String SYSPROP_COUNT_THREADS          = "COUNT_THREADS";
    
    public static void main(String[] arguments) {
        Scanner inScan = null;
        int primaries = -1;
        int secondariesPerPrimary = -1;
        int threads = -1;
        final GraphBuilder gb;
        
        try {
            primaries = Integer.parseInt(System.getProperty(SYSPROP_COUNT_VERTEX_PRIMARY));
        } catch (NullPointerException npExc) {
            // ignore
        }
        try {
            secondariesPerPrimary = Integer.parseInt(System.getProperty(SYSPROP_COUNT_VERTEX_SECONDARY));
        } catch (NullPointerException npExc) {
            // ignore
        }
        try {
            threads = Integer.parseInt(System.getProperty(SYSPROP_COUNT_THREADS));
        } catch (NullPointerException npExc) {
            // ignore
        }
        
        try {
            inScan = new Scanner(System.in);
            if (primaries < 0) {
                System.out.print("Number of PRIMARY vertices: ");
                primaries = Integer.parseInt(inScan.nextLine());
            }
            if (secondariesPerPrimary < 0) {
                System.out.print("Number of SECONDARY vertices per primary: ");
                secondariesPerPrimary = Integer.parseInt(inScan.nextLine());
            }
            if (threads < 0) {
                System.out.print("Concurrent threads: ");
                threads = Integer.parseInt(inScan.nextLine());
            }
            gb = new GraphBuilder(primaries, secondariesPerPrimary, threads);
            gb.build();
        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            if (inScan != null) {
                inScan.close();
            }
        }
    }
}
