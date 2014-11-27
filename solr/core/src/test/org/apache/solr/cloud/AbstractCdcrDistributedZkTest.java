package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.params.CoreConnectionPNames;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.CdcrRequestHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.CreateMode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * <p>
 *  Abstract class for CDCR unit testing. This class emulates two clusters, a source and target, by using different
 *  collections in the same SolrCloud cluster. Therefore, the two clusters will share the same Zookeeper cluster. In
 *  real scenario, the two collections/clusters will likely have their own zookeeper cluster.
 * </p>
 * <p>
 *   This class will automatically create two collections, the source and the target. Each collection will have
 *   {@link #sliceCount} slices, and {@link #replicationFactor} replicas per slice. One jetty instance will
 *   be created per core.
 * </p>
 * <p>
 *   The source and target collection can be reinitialised at will by calling {@link #clearSourceCollection()} and
 *   {@link #clearTargetCollection()}. After reinitialisation, a collection will have a new fresh index and update log.
 * </p>
 * <p>
 *   Servers can be restarted at will by calling
 *   {@link #restartServer(org.apache.solr.cloud.AbstractCdcrDistributedZkTest.CloudJettyRunner)} or
 *   {@link #restartServers(java.util.List)}.
 * </p>
 * <p>
 *   The creation of the target collection can be disabled with the flag {@link #createTargetCollection};
 * </p>
 * <p>
 *   NB: We cannot use multiple cores per jetty instance, as jetty will load only one core when restarting. It seems
 *   that this is a limitation of the {@link org.apache.solr.client.solrj.embedded.JettySolrRunner}. This class
 *   tries to ensure that there always is one single core per jetty instance.
 * </p>
 */
public abstract class AbstractCdcrDistributedZkTest extends AbstractDistribZkTestBase {

  protected int sliceCount = 2;
  protected int replicationFactor = 2;
  protected boolean createTargetCollection = true;

  private static final String CDCR_PATH = "/cdcr";

  protected static final String SOURCE_COLLECTION = "source_collection";
  protected static final String TARGET_COLLECTION = "target_collection";

  public static final String SHARD1 = "shard1";
  public static final String SHARD2 = "shard2";

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-cdcr.xml";
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    if (isSSLMode()) {
      System.clearProperty("urlScheme");
      ZkStateReader zkStateReader = new ZkStateReader(zkServer.getZkAddress(),
          AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT);
      try {
        zkStateReader.getZkClient().create(ZkStateReader.CLUSTER_PROPS,
            ZkStateReader.toJSON(Collections.singletonMap("urlScheme", "https")),
            CreateMode.PERSISTENT, true);
      } finally {
        zkStateReader.close();
      }
    }
  }

  @Test
  public void testDistribSearch() throws Exception {
    this.createSourceCollection();
    if (this.createTargetCollection) this.createTargetCollection();
    RandVal.uniqueValues = new HashSet(); //reset random values
    doTest();
    destroyServers();
  }

  protected CloudSolrServer createCloudClient(String defaultCollection) {
    CloudSolrServer server = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
    server.setParallelUpdates(random().nextBoolean());
    if (defaultCollection != null) server.setDefaultCollection(defaultCollection);
    server.getLbServer().getHttpClient().getParams()
        .setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 30000);
    return server;
  }

  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }

  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  protected void index(String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      client.add(doc);
      client.commit(true, true);
    }
    finally {
      client.shutdown();
    }
  }

  protected void index(String collection, List<SolrInputDocument> docs) throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      client.add(docs);
      client.commit(true, true);
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Invokes a commit on the given collection.
   */
  protected void commit(String collection) throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      client.commit(true, true);
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Returns the number of documents in a given collection
   */
  protected long getNumDocs(String collection) throws SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      return client.query(new SolrQuery("*:*")).getResults().getNumFound();
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Invokes a CDCR action on a given node.
   */
  protected NamedList invokeCdcrAction(CloudJettyRunner jetty, CdcrRequestHandler.CdcrAction action) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, action.toString());

    SolrRequest request = new QueryRequest(params);
    request.setPath(CDCR_PATH);

    return jetty.client.request(request);
  }

  /**
   * Assert the state of CDCR on each nodes of the given collection.
   */
  protected void assertState(String collection, CdcrRequestHandler.ProcessState processState, CdcrRequestHandler.BufferState bufferState)
  throws Exception {
    for (CloudJettyRunner jetty : cloudJettys.get(collection)) { // check all replicas
      NamedList rsp = invokeCdcrAction(jetty, CdcrRequestHandler.CdcrAction.STATUS);
      NamedList status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
      assertEquals(processState.toLower(), status.get(CdcrRequestHandler.ProcessState.getParam()));
      assertEquals(bufferState.toLower(), status.get(CdcrRequestHandler.BufferState.getParam()));
    }
  }

  /**
   * A mapping between collection and node names. This is used when creating the collection in
   * {@link #createCollection(String)}.
   */
  private Map<String, List<String>> collectionToNodeNames = new HashMap<>();

  /**
   * Starts the servers, saves and associates the node names to the source collection,
   * and finally creates the source collection.
   */
  private void createSourceCollection() throws Exception {
    List<String> nodeNames = this.startServers(sliceCount * replicationFactor);
    this.collectionToNodeNames.put(SOURCE_COLLECTION, nodeNames);
    this.createCollection(SOURCE_COLLECTION);
    this.waitForRecoveriesToFinish(SOURCE_COLLECTION, true);
    this.updateMappingsFromZk(SOURCE_COLLECTION);
  }

  /**
   * Clear the source collection. It will delete then create the collection through the collection API.
   * The collection will have a new fresh index, i.e., including a new update log.
   */
  protected void clearSourceCollection() throws Exception {
    this.deleteCollection(SOURCE_COLLECTION);
    this.createCollection(SOURCE_COLLECTION);
    this.waitForRecoveriesToFinish(SOURCE_COLLECTION, true);
    this.updateMappingsFromZk(SOURCE_COLLECTION);
  }

  /**
   * Starts the servers, saves and associates the node names to the target collection,
   * and finally creates the target collection.
   */
  private void createTargetCollection() throws Exception {
    List<String> nodeNames = this.startServers(sliceCount * replicationFactor);
    this.collectionToNodeNames.put(TARGET_COLLECTION, nodeNames);
    this.createCollection(TARGET_COLLECTION);
    this.waitForRecoveriesToFinish(TARGET_COLLECTION, true);
    this.updateMappingsFromZk(TARGET_COLLECTION);
  }

  /**
   * Clear the source collection. It will delete then create the collection through the collection API.
   * The collection will have a new fresh index, i.e., including a new update log.
   */
  protected void clearTargetCollection() throws Exception {
    this.deleteCollection(TARGET_COLLECTION);
    this.createCollection(TARGET_COLLECTION);
    this.waitForRecoveriesToFinish(TARGET_COLLECTION, true);
    this.updateMappingsFromZk(TARGET_COLLECTION);
  }

  /**
   * Create a new collection through the Collection API. It enforces the use of one max shard per node.
   * It will define the nodes to spread the new collection across by using the mapping {@link #collectionToNodeNames},
   * to ensure that a node will not host more than one core (which will create problem when trying to restart servers).
   */
  private void createCollection(String name) throws Exception {
    CloudSolrServer client = createCloudClient(null);
    try {
      // Create the target collection
      Map<String, List<Integer>> collectionInfos = new HashMap<>();
      int maxShardsPerNode = 1;

      StringBuilder sb = new StringBuilder();
      for (String nodeName : collectionToNodeNames.get(name)) {
        sb.append(nodeName);
        sb.append(',');
      }
      sb.deleteCharAt(sb.length() - 1);

      createCollection(collectionInfos, name, sliceCount, replicationFactor, maxShardsPerNode, client, sb.toString());
    }
    finally {
      client.shutdown();
    }
  }

  private CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos,
                                                   String collectionName, int numShards, int replicationFactor,
                                                   int maxShardsPerNode, SolrServer client, String createNodeSetStr)
      throws SolrServerException, IOException {
    return createCollection(collectionInfos, collectionName,
        ZkNodeProps.makeMap(
            NUM_SLICES, numShards,
            REPLICATION_FACTOR, replicationFactor,
            CREATE_NODE_SET, createNodeSetStr,
            MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client, null);
  }

  private CollectionAdminResponse createCollection(Map<String, List<Integer>> collectionInfos, String collectionName,
                                                   Map<String, Object> collectionProps, SolrServer client,
                                                   String confSetName)
      throws SolrServerException, IOException{
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.CREATE.toString());
    for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
      if(entry.getValue() !=null) params.set(entry.getKey(), String.valueOf(entry.getValue()));
    }
    Integer numShards = (Integer) collectionProps.get(NUM_SLICES);
    if(numShards==null){
      String shardNames = (String) collectionProps.get(SHARDS_PROP);
      numShards = StrUtils.splitSmart(shardNames, ',').size();
    }
    Integer replicationFactor = (Integer) collectionProps.get(REPLICATION_FACTOR);
    if(replicationFactor==null){
      replicationFactor = (Integer) OverseerCollectionProcessor.COLL_PROPS.get(REPLICATION_FACTOR);
    }

    if (confSetName != null) {
      params.set("collection.configName", confSetName);
    }

    List<Integer> list = new ArrayList<>();
    list.add(numShards);
    list.add(replicationFactor);
    if (collectionInfos != null) {
      collectionInfos.put(collectionName, list);
    }
    params.set("name", collectionName);
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse(client.request(request));
    return res;
  }

  /**
   * Delete a collection through the Collection API.
   */
  private CollectionAdminResponse deleteCollection(String collectionName) throws SolrServerException, IOException {
    SolrServer client = createCloudClient(null);
    CollectionAdminResponse res;

    try {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.DELETE.toString());
      params.set("name", collectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");

      res = new CollectionAdminResponse();
      res.setResponse(client.request(request));
    }
    finally {
      client.shutdown();
    }

    return res;
  }

  private void waitForRecoveriesToFinish(String collection, boolean verbose) throws Exception {
    CloudSolrServer client = this.createCloudClient(null);
    try {
      client.connect();
      ZkStateReader zkStateReader = client.getZkStateReader();
      super.waitForRecoveriesToFinish(collection, zkStateReader, verbose);
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Asserts that the collection has the correct number of shards and replicas
   */
  protected void assertCollectionExpectations(String collectionName) {
    CloudSolrServer client = this.createCloudClient(null);
    try {
      client.connect();
      ClusterState clusterState = client.getZkStateReader().getClusterState();

      assertTrue("Could not find new collection " + collectionName, clusterState.hasCollection(collectionName));
      Map<String, Slice> slices = clusterState.getCollection(collectionName).getSlicesMap();
      // did we find expectedSlices slices/shards?
      assertEquals("Found new collection " + collectionName + ", but mismatch on number of slices.", sliceCount, slices.size());
      int totalShards = 0;
      for (String sliceName : slices.keySet()) {
        totalShards += slices.get(sliceName).getReplicas().size();
      }
      int expectedTotalShards = sliceCount * replicationFactor;
      assertEquals("Found new collection " + collectionName + " with correct number of slices, but mismatch on number " +
          "of shards.", expectedTotalShards, totalShards);
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Restart a server.
   */
  protected void restartServer(CloudJettyRunner server) throws Exception {
    // it seems we need to set the collection property to have the jetty properly restarted
    System.setProperty("collection", server.collection);
    JettySolrRunner jetty = server.jetty;
    ChaosMonkey.stop(jetty);
    ChaosMonkey.start(jetty);
    System.clearProperty("collection");
    waitForRecoveriesToFinish(server.collection, true);
    updateMappingsFromZk(server.collection); // must update the mapping as the core node name might have changed
  }

  /**
   * Restarts a list of servers.
   */
  protected void restartServers(List<CloudJettyRunner> servers) throws Exception {
    for (CloudJettyRunner server : servers) {
      this.restartServer(server);
    }
  }

  private AtomicInteger homeCount = new AtomicInteger();
  private List<JettySolrRunner> jettys = new ArrayList<>();

  /**
   * Creates and starts a given number of servers.
   */
  protected List<String> startServers(int nServer) throws Exception {
    String temporaryCollection = "tmp_collection";
    System.setProperty("collection", temporaryCollection);
    for (int i = 1; i <= nServer; i++) {
      // give everyone there own solrhome
      File jettyHome = new File(new File(getSolrHome()).getParentFile(), "jetty" + homeCount.incrementAndGet());
      setupJettySolrHome(jettyHome);
      JettySolrRunner jetty = createJetty(jettyHome, null, "shard" + i);
      jettys.add(jetty);
    }

    ZkStateReader zkStateReader = ((SolrDispatchFilter) jettys.get(0)
        .getDispatchFilter().getFilter()).getCores().getZkController()
        .getZkStateReader();

    // now wait till we see the leader for each shard
    for (int i = 1; i <= sliceCount; i++) {
      zkStateReader.getLeaderRetry(temporaryCollection, "shard" + i, 15000);
    }

    // store the node names
    List<String> nodeNames = new ArrayList<>();
    for (Slice slice : zkStateReader.getClusterState().getCollection(temporaryCollection).getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        nodeNames.add(replica.getNodeName());
      }
    }

    // delete the temporary collection - we will create our own collections later
    this.deleteCollection(temporaryCollection);
    System.clearProperty("collection");

    return nodeNames;
  }

  @Override
  protected void destroyServers() throws Exception {
    for (JettySolrRunner runner : jettys) {
      try {
        ChaosMonkey.stop(runner);
      } catch (Exception e) {
        log.error("", e);
      }
    }

    jettys.clear();
  }

  /**
<<<<<<< HEAD
   *
   * getLeader returns e.g. http://127.0.0.1:59803/b_k/myCollection_shard1_replica1/
   * while getReplicaUrls returns e.g. http://127.0.0.1:59803/b_k
   *
   */
  protected String getTruncatedLeaderUrl(String collection, String shard) throws Exception {
    String leader = getLeaderUrl(collection, shard);

    String tmp = leader.substring(0, leader.length()-1);
    leader = tmp.substring(0, tmp.lastIndexOf('/'));

    return leader;
  }

  /**
   * Creates a non-smart client to a replica (non-leader).
   */
  protected SolrServer getReplicaClient(String collection, String shard) throws Exception {
    List<String> replicas = getReplicaUrls(collection, shard);

    replicas.remove(getTruncatedLeaderUrl(collection, shard));
    return createNewSolrServer(collection, replicas.get(0));
  }
=======
   * Mapping from collection to jettys
   */
  protected Map<String, List<CloudJettyRunner>> cloudJettys = new HashMap<>();

  /**
   * Mapping from collection/shard to jettys
   */
  protected Map<String, Map<String, List<CloudJettyRunner>>> shardToJetty = new HashMap<>();
>>>>>>> Fixed test framework. Fixed racing condition in leader state manager. Fixed bug in update log initialisation.

  /**
   * Mapping from collection/shard leader to jettys
   */
  protected Map<String, Map<String, CloudJettyRunner>> shardToLeaderJetty = new HashMap<>();

  /**
   * Updates the mappings between the jetty's instances and the zookeeper cluster state.
   */
  protected void updateMappingsFromZk(String collection) throws Exception {
    List<CloudJettyRunner> cloudJettys = new ArrayList<>();
    Map<String,List<CloudJettyRunner>> shardToJetty = new HashMap<>();
    Map<String,CloudJettyRunner> shardToLeaderJetty = new HashMap<>();

    CloudSolrServer cloudClient = this.createCloudClient(null);
    try {
      cloudClient.connect();
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollection(collection);

      for (JettySolrRunner jetty : jettys) {
        int port = jetty.getLocalPort();
        if (port == -1) {
          throw new RuntimeException("Cannot find the port for jetty");
        }

        nextJetty:
        for (Slice slice : coll.getSlices()) {
          Set<Map.Entry<String, Replica>> entries = slice.getReplicasMap().entrySet();
          for (Map.Entry<String, Replica> entry : entries) {
            Replica replica = entry.getValue();
            if (replica.getStr(ZkStateReader.BASE_URL_PROP).contains(":" + port)) {
              List<CloudJettyRunner> list = shardToJetty.get(slice.getName());
              if (list == null) {
                list = new ArrayList<>();
                shardToJetty.put(slice.getName(), list);
              }
              boolean isLeader = slice.getLeader() == replica;
              CloudJettyRunner cjr = new CloudJettyRunner(jetty, replica, collection, slice.getName(), entry.getKey());
              list.add(cjr);
              if (isLeader) {
                shardToLeaderJetty.put(slice.getName(), cjr);
              }
              cloudJettys.add(cjr);
              break nextJetty;
            }
          }
        }
      }

      this.cloudJettys.put(collection, cloudJettys);
      this.shardToJetty.put(collection, shardToJetty);
      this.shardToLeaderJetty.put(collection, shardToLeaderJetty);
    }
    finally {
      cloudClient.shutdown();
    }
  }

  /**
   * Wrapper around a {@link org.apache.solr.client.solrj.embedded.JettySolrRunner} to map the jetty
   * instance to various information of the cloud cluster, such as the collection and slice
   * that is served by the jetty instance, the node name, core node name, url, etc.
   */
  public static class CloudJettyRunner {

    public JettySolrRunner jetty;
    public String nodeName;
    public String coreNodeName;
    public String url;
    public SolrServer client;
    public Replica info;
    public String slice;
    public String collection;

    public CloudJettyRunner(JettySolrRunner jetty, Replica replica,
                            String collection, String slice, String coreNodeName) {
      this.jetty = jetty;
      this.info = replica;
      this.collection = collection;

      // we need to update the jetty's shard so that it registers itself to the right shard when restarted
      this.slice = slice;
      this.jetty.setShards(this.slice);

      // we need to update the jetty's shard so that it registers itself under the right core name when restarted
      this.coreNodeName = coreNodeName;
      this.jetty.setCoreNodeName(this.coreNodeName);

      this.nodeName = replica.getNodeName();

      ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(info);
      this.url = coreNodeProps.getCoreUrl();

      // strip the trailing slash as this can cause issues when executing requests
      this.client = createNewSolrServer(this.url.substring(0, this.url.length() - 1));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((url == null) ? 0 : url.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      CloudJettyRunner other = (CloudJettyRunner) obj;
      if (url == null) {
        if (other.url != null) return false;
      } else if (!url.equals(other.url)) return false;
      return true;
    }

    @Override
    public String toString() {
      return "CloudJettyRunner [url=" + url + "]";
    }

  }

  protected static SolrServer createNewSolrServer(String baseUrl) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(baseUrl);
      s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
