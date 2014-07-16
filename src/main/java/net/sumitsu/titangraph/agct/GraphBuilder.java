package net.sumitsu.titangraph.agct;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sumitsu.titangraph.agct.util.StaticUtil;

import org.apache.log4j.Logger;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration.Restriction;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.diskstorage.es.ElasticSearchIndex;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.types.ParameterType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class GraphBuilder {
    
    public static final String SYSPROP_STORAGE_BACKEND       = "TITAN_STORAGE_BACKEND";
    public static final String SYSPROP_ELASTICSEARCH_HOST    = "TITAN_ELASTICSEARCH_HOST";
    public static final String SYSPROP_ELASTICSEARCH_CLUSTER = "TITAN_ELASTICSEARCH_CLUSTER";
    public static final String SYSPROP_STORAGE_HOST          = "TITAN_STORAGE_HOST";
    public static final String SYSPROP_CASSANDRA_KEYSPACE    = "TITAN_CASSANDRA_KEYSPACE";
    public static final String SYSPROP_HBASE_TABLE           = "TITAN_HBASE_TABLE";
    public static final String SYSPROP_TITAN_INDEXNAME       = "TITAN_INDEX_NAME";
    
    public static final String BACKEND_CASSANDRA = "cassandra";
    public static final String BACKEND_HBASE     = "hbase";
    
    public static final String INDEX_ELASTICSEARCH = "elasticsearch";
    
    public static final String VERTEXPROP_TYPE              = "vertexType";
    public static final String INDEX_VERTEXPROP_TYPE        = "index." + VERTEXPROP_TYPE;
    public static final String VERTEXPROP_PRIMARYID         = "primaryID";
    public static final String INDEX_VERTEXPROP_PRIMARYID   = "index." + VERTEXPROP_PRIMARYID;
    public static final String VERTEXPROP_SECONDARYID       = "secondaryID";
    public static final String INDEX_VERTEXPROP_SECONDARYID = "index." + VERTEXPROP_SECONDARYID;
    
    public static final String EDGEPROP_TYPE       = "edgeType";
    public static final String INDEX_EDGEPROP_TYPE = "index." + EDGEPROP_TYPE;

    private final Logger log;
    private final int primaries;
    private final int secondariesPerPrimary;
    private final int threads;
    
    private TitanGraph startGraph() throws IllegalArgumentException {
        final String methodName;

        final String envBackendType;
        final String envIndexName;
        final String envStorageHost;
        final String envCassandraKeyspace;
        final String envHBaseTable;
        final String envElasticsearchHost;
        final String envElasticsearchClusterName;
        
        final ModifiableConfiguration graphConfig;
        final TitanGraph graph;
        final TitanManagement graphMgt;
        PropertyKeyMaker maker;
        final PropertyKey propKeyVertexType;
        final PropertyKey propKeyEdgeType;
        final PropertyKey propKeyPrimaryID;
        final PropertyKey propKeySecondaryID;
        final TitanGraphIndex indexPropKeyVertexType;
        final TitanGraphIndex indexPropKeyEdgeType;
        final TitanGraphIndex indexPropKeyPrimaryID;
        final TitanGraphIndex indexPropKeySecondaryID;
        
        methodName = "startGraph";
        if (log.isDebugEnabled()) { log.debug(">>> " + methodName); }
        
        envIndexName = StaticUtil.simplify(System.getProperty(SYSPROP_TITAN_INDEXNAME));
        envBackendType = StaticUtil.simplify(System.getProperty(SYSPROP_STORAGE_BACKEND));
        envStorageHost = StaticUtil.simplify(System.getProperty(SYSPROP_STORAGE_HOST));
        envCassandraKeyspace = StaticUtil.simplify(System.getProperty(SYSPROP_CASSANDRA_KEYSPACE));
        envHBaseTable = StaticUtil.simplify(System.getProperty(SYSPROP_HBASE_TABLE));
        envElasticsearchHost = StaticUtil.simplify(System.getProperty(SYSPROP_ELASTICSEARCH_HOST));
        envElasticsearchClusterName = StaticUtil.simplify(System.getProperty(SYSPROP_ELASTICSEARCH_CLUSTER));
        
        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] "
                      + "envIndexName=" + envIndexName
                      + ",envBackendType=" + envBackendType
                      + ",envStorageHost=" + envStorageHost
                      + ",envCassandraKeyspace=" + envCassandraKeyspace
                      + ",envHBaseTable=" + envHBaseTable
                      + ",envElasticsearchHost=" + envElasticsearchHost
                      + ",envElasticsearchClusterName=" + envElasticsearchClusterName);
        }

        graphConfig =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS,
                                        new CommonsConfiguration(),
                                        Restriction.NONE);

        graphConfig.set(GraphDatabaseConfiguration.STORAGE_BACKEND, envBackendType);
        graphConfig.set(GraphDatabaseConfiguration.STORAGE_HOSTS, new String[]{envStorageHost});
        if (BACKEND_CASSANDRA.equals(envBackendType)) {
            // configure Cassandra backend
            graphConfig.set(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE, envCassandraKeyspace);
        } else if (BACKEND_HBASE.equals(envBackendType)) {
            // configure HBase/MapR backend
            graphConfig.set(HBaseStoreManager.HBASE_TABLE, envHBaseTable);
        } else {
            throw new IllegalArgumentException("Backend type not supported by application: " + envBackendType);
        }
        
        // configure Elasticsearch index
        graphConfig.set(GraphDatabaseConfiguration.INDEX_BACKEND, INDEX_ELASTICSEARCH);
        graphConfig.set(GraphDatabaseConfiguration.INDEX_HOSTS, new String[]{envElasticsearchHost});
        graphConfig.set(GraphDatabaseConfiguration.INDEX_NAME, envIndexName);
        graphConfig.set(ElasticSearchIndex.CLUSTER_NAME, envElasticsearchClusterName);
        graphConfig.set(ElasticSearchIndex.LOCAL_MODE, false);
        graphConfig.set(ElasticSearchIndex.CLIENT_ONLY, true);
        
        graph = TitanFactory.open(graphConfig);
        graphMgt = graph.getManagementSystem();
        
        if (log.isDebugEnabled()) { log.debug("[" + methodName + "] graph open"); }
        
        /*
         * KEY + INDEX (string): vertexType (vertex property)
         */
        maker = graph.makePropertyKey(VERTEXPROP_TYPE);
        maker.dataType(String.class);
        propKeyVertexType = maker.make();
        
        indexPropKeyVertexType =
            graphMgt
                .buildIndex(INDEX_VERTEXPROP_TYPE, Vertex.class)
                .indexKey(propKeyVertexType,
                          Parameter.of(ParameterType.MAPPING.toString(), Mapping.STRING))
                .buildMixedIndex(envIndexName);

        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] key created: "
                      + VERTEXPROP_TYPE
                      + "(key=" + propKeyVertexType + ",index=" + indexPropKeyVertexType + ")");
        }
        
        /*
         * KEY + INDEX (string): edgeType (edge property)
         */
        maker = graph.makePropertyKey(EDGEPROP_TYPE);
        maker.dataType(String.class);
        propKeyEdgeType = maker.make();
        
        indexPropKeyEdgeType =
            graphMgt
                .buildIndex(INDEX_EDGEPROP_TYPE, Edge.class)
                .indexKey(propKeyEdgeType,
                          Parameter.of(ParameterType.MAPPING.toString(), Mapping.STRING))
                .buildMixedIndex(envIndexName);

        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] key created: "
                      + EDGEPROP_TYPE
                      + "(key=" + propKeyEdgeType + ",index=" + indexPropKeyEdgeType + ")");
        }
        
        /*
         * KEY + UNIQUE INDEX (string): primaryID (vertex property)
         */
        maker = graph.makePropertyKey(VERTEXPROP_PRIMARYID);
        maker.dataType(String.class);
        propKeyPrimaryID = maker.make();
        
        indexPropKeyPrimaryID =
            graphMgt
                .buildIndex(INDEX_VERTEXPROP_PRIMARYID, Vertex.class)
                .indexKey(propKeyPrimaryID,
                          Parameter.of(ParameterType.MAPPING.toString(), Mapping.STRING))
                .unique()
                .buildMixedIndex(envIndexName);

        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] unique key created: "
                      + VERTEXPROP_PRIMARYID
                      + "(key=" + propKeyPrimaryID + ",index=" + indexPropKeyPrimaryID + ")");
        }
        
        /*
         * KEY + UNIQUE INDEX (string): secondaryID (vertex property)
         */
        maker = graph.makePropertyKey(VERTEXPROP_SECONDARYID);
        maker.dataType(String.class);
        propKeySecondaryID = maker.make();
        
        indexPropKeySecondaryID =
            graphMgt
                .buildIndex(INDEX_VERTEXPROP_SECONDARYID, Vertex.class)
                .indexKey(propKeySecondaryID,
                          Parameter.of(ParameterType.MAPPING.toString(), Mapping.STRING))
                .unique()
                .buildMixedIndex(envIndexName);

        if (log.isDebugEnabled()) {
            log.debug("[" + methodName + "] unique key created: "
                      + VERTEXPROP_SECONDARYID
                      + "(key=" + propKeySecondaryID + ",index=" + indexPropKeySecondaryID + ")");
        }
        
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
