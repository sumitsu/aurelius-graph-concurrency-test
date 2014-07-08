package net.sumitsu.titangraph.agct;

import java.util.Collections;

import org.apache.log4j.Logger;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class VertexPopulationThread implements Runnable {

    final Logger log;
    final TitanGraph graph;
    final RecordIdentifier ri;

    private void populateGraph() {
        TitanGraphQuery tgQuery;
        Iterable<Vertex> matches;
        Vertex v;
        Edge e;
        
        /*
         * check: exists(SECONDARY)?
         * if exists(SECONDARY) --> exit
         */
        tgQuery = this.graph.query();
        tgQuery.has(GraphBuilder.VERTEXPROP_TYPE, VertexType.SECONDARY.toString());
        tgQuery.has(GraphBuilder.VERTEXPROP_SECONDARYID, this.ri.getSecondary());
        matches = tgQuery.vertices();
        if ((matches == null) || (!matches.iterator().hasNext())) {
            if (log.isDebugEnabled()) { log.debug("creating secondary: " + ri + "..."); }
            
            // create SECONDARY
            v = this.graph.addVertex(VertexType.SECONDARY + "." + this.ri.getSecondary());
            v.setProperty(GraphBuilder.VERTEXPROP_TYPE, VertexType.SECONDARY.toString());
            v.setProperty(GraphBuilder.VERTEXPROP_SECONDARYID, this.ri.getSecondary());

            if (log.isDebugEnabled()) { log.debug("secondary created: " + ri); }
            
            // check: exists(PRIMARY)?
            tgQuery = this.graph.query();
            tgQuery.has(GraphBuilder.VERTEXPROP_TYPE, VertexType.PRIMARY.toString());
            tgQuery.has(GraphBuilder.VERTEXPROP_PRIMARYID, this.ri.getPrimary());
            matches = tgQuery.vertices();
            if ((matches == null) || (!matches.iterator().hasNext())) {
                if (log.isDebugEnabled()) { log.debug("creating primary: " + ri + "..."); }
                /*
                 * if NOT exists(PRIMARY):
                 *     create PRIMARY
                 */
                v = this.graph.addVertex(VertexType.PRIMARY + "." + this.ri.getPrimary());
                v.setProperty(GraphBuilder.VERTEXPROP_TYPE, VertexType.PRIMARY.toString());
                v.setProperty(GraphBuilder.VERTEXPROP_PRIMARYID, this.ri.getPrimary());
                matches = Collections.singleton(v);
                if (log.isDebugEnabled()) { log.debug("primary created: " + ri); }
            } else {
                if (log.isDebugEnabled()) { log.debug("primary already exists: " + ri); }
            }
            
            /*
             * create edge: PRIMARY--[sub]-->SECONDARY
             * create edge: SECONDARY-->[super]-->PRIMARY
             */
            for (Vertex superNode : matches) {
                if (superNode != null) {
                    e = superNode.addEdge(EdgeType.SUPER_OF.toString(), v);
                    e.setProperty(GraphBuilder.EDGEPROP_TYPE, EdgeType.SUPER_OF);
                    e = v.addEdge(EdgeType.SUB_OF.toString(), superNode);
                    e.setProperty(GraphBuilder.EDGEPROP_TYPE, EdgeType.SUB_OF);
                    if (log.isDebugEnabled()) {
                        log.debug("linked primary(" + superNode + ") and secondary(" + v + ") with edge(" + e + ") for: " + ri);
                    }
                }
            }
            if (log.isDebugEnabled()) { log.debug("committing: " + ri + "..."); }
            this.graph.commit();
            if (log.isDebugEnabled()) { log.debug("COMMIT SUCCESS: " + ri); }
        } else {
            if (log.isDebugEnabled()) { log.debug("secondary already exists: " + ri); }
        }
    }
    
    @Override
    public void run() {
        try {
            populateGraph();
        } catch (Exception exc) {
            log.error("FAILED to populate to graph: " + this.ri + "; " + exc, exc);
            this.graph.rollback();
        }
    }
    
    public VertexPopulationThread(final TitanGraph graph, final RecordIdentifier ri) {
        this.log = Logger.getLogger(getClass());
        this.graph = graph;
        this.ri = ri;
    }
}
