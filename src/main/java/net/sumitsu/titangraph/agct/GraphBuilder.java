package net.sumitsu.titangraph.agct;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.Mapping;
import com.thinkaurelius.titan.core.Parameter;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class GraphBuilder {
    
    public static final String SYSPROP_ELASTICSEARCH_HOST    = "TITAN_ELASTICSEARCH_HOST";
    public static final String SYSPROP_ELASTICSEARCH_CLUSTER = "TITAN_ELASTICSEARCH_CLUSTER";
    public static final String SYSPROP_CASSANDRA_HOST        = "TITAN_CASSANDRA_HOST";
    public static final String SYSPROP_TITAN_INDEXNAME       = "TITAN_INDEX_NAME";
    
    public static final String VERTEXPROP_TYPE        = "vertexType";
    public static final String VERTEXPROP_PRIMARYID   = "primaryID";
    public static final String VERTEXPROP_SECONDARYID = "secondaryID";
    
    public static final String EDGEPROP_TYPE = "edgeType";

    private final Logger log;
    private final int primaries;
    private final int secondariesPerPrimary;
    private final int threads;
    
    private TitanGraph startGraph() {
        final String methodName;

        final String envIndexName;
        final String envCassandraHost;
        final String envElasticsearchHost;
        final String envElasticsearchClusterName;
        
        final BaseConfiguration config;
        final Configuration storage;
        final Configuration index;
        final TitanGraph graph;
        KeyMaker maker;
        
        methodName = "startGraph";
        if (log.isDebugEnabled()) { log.debug(">>> " + methodName); }
        
        envIndexName = System.getProperty(SYSPROP_TITAN_INDEXNAME);
        envCassandraHost = System.getProperty(SYSPROP_CASSANDRA_HOST);
        envElasticsearchHost = System.getProperty(SYSPROP_ELASTICSEARCH_HOST);
        envElasticsearchClusterName = System.getProperty(SYSPROP_ELASTICSEARCH_CLUSTER);
        
        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] "
                      + "envIndexName=" + envIndexName
                      + ",envCassandraHost=" + envCassandraHost
                      + ",envElasticsearchHost=" + envElasticsearchHost
                      + ",envElasticsearchClusterName=" + envElasticsearchClusterName);
        }
        
        config = new BaseConfiguration();
        storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
        // configure Cassandra backend
        storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "cassandra");
        storage.setProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, envCassandraHost);
        // configure Elasticsearch index
        index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(envIndexName);
        index.setProperty("index-name", envIndexName);
        index.setProperty(GraphDatabaseConfiguration.INDEX_BACKEND_KEY, "elasticsearch");
        index.setProperty(GraphDatabaseConfiguration.HOSTNAME_KEY, envElasticsearchHost);
        index.setProperty("cluster-name", envElasticsearchClusterName);
        index.setProperty("local-mode", false);
        index.setProperty("client-only", true);
        
        graph = TitanFactory.open(config);
        
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] graph open"); }
        
        maker = graph.makeKey(VERTEXPROP_TYPE);
        maker.dataType(String.class);
        maker.indexed(envIndexName,
                      Vertex.class,
                      Parameter.of(Mapping.MAPPING_PREFIX, Mapping.STRING));
        maker.make();
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] key created: " + VERTEXPROP_TYPE); }
        
        maker = graph.makeKey(EDGEPROP_TYPE);
        maker.dataType(String.class);
        maker.indexed(envIndexName,
                      Edge.class,
                      Parameter.of(Mapping.MAPPING_PREFIX, Mapping.STRING));
        maker.make();
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] key created: " + EDGEPROP_TYPE); }
        
        maker = graph.makeKey(VERTEXPROP_PRIMARYID);
        maker.dataType(String.class);
        maker.indexed(envIndexName,
                      Vertex.class,
                      Parameter.of(Mapping.MAPPING_PREFIX, Mapping.STRING));
        maker.indexed(Vertex.class);
        maker.unique();
        maker.make();
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] unique key created: " + VERTEXPROP_PRIMARYID); }
        
        maker = graph.makeKey(VERTEXPROP_SECONDARYID);
        maker.dataType(String.class);
        maker.indexed(envIndexName,
                      Vertex.class,
                      Parameter.of(Mapping.MAPPING_PREFIX, Mapping.STRING));
        maker.indexed(Vertex.class);
        maker.unique();
        maker.make();
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] unique key created: " + VERTEXPROP_SECONDARYID); }
        
        return graph;
    }
    
    public void build() {
        final ExecutorService execServ;
        final TitanGraph graph;
        final VertexPlaceKeeper vpk;
        RecordIdentifier ri;
        
        graph = startGraph();
        
        execServ = Executors.newFixedThreadPool(this.threads);
        vpk = new VertexPlaceKeeper(this.primaries, this.secondariesPerPrimary);
        do {
            ri = vpk.dequeue();
            if (ri != null) {
                if (log.isDebugEnabled()) { log.debug("dequeued: " + ri); }
                execServ.submit(new VertexPopulationThread(graph, ri));
                if (log.isDebugEnabled()) { log.debug("submitted: " + ri); }
            }
        } while (ri != null);
    }
    
    public GraphBuilder(final int primaries, final int secondariesPerPrimary, final int threads) {
        this.log = Logger.getLogger(getClass());
        this.primaries = primaries;
        this.secondariesPerPrimary = secondariesPerPrimary;
        this.threads = threads;
    }
}
