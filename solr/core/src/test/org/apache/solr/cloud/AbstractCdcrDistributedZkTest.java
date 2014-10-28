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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrRequestHandler;

import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

/**
 * <p>
 *  Abstract class for CDCR unit testing. This class emulates two clusters, a source and target, by using different
 *  collections in the same SolrCloud cluster. Therefore, the two clusters will share the same Zookeeper cluster. In
 *  real scenario, the two collections/clusters will likely have their own zookeeper cluster.
 * </p>
 * <p>
 *  This class provides a method {@link #createTargetCollection()} to create
 *  a source and target collection that will be used as a source and target cluster respectively. The source collection
 *  is the default collection {@link #SOURCE_COLLECTION} and the target collection is {@link #TARGET_COLLECTION}.
 * </p>
 * <p>
 *  Each collection is configured with 2 slices and 4 replicas. These collection will be distributed across 4 jetty
 *  servers. Therefore, each jetty server will host two cores, one core for each collection.
 * </p>
 */
public abstract class AbstractCdcrDistributedZkTest extends AbstractFullDistribZkTestBase {

  private static final String CDCR_PATH = "/cdcr";

  protected static final String SOURCE_COLLECTION = "collection1";
  protected static final String TARGET_COLLECTION = "target_collection";

  public AbstractCdcrDistributedZkTest() {
    fixShardCount = true;
    sliceCount = 2;
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-cdcr.xml";
  }

  protected void createCollection(String name) throws Exception {
    CloudSolrServer client = createCloudClient(null);

    // Create the target collection
    Map<String,List<Integer>> collectionInfos = new HashMap<>();
    int numShards = sliceCount;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrServer()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    createCollection(collectionInfos, name, numShards, replicationFactor, maxShardsPerNode, client, null);

    if (client != null) client.shutdown();
  }

  protected void createTargetCollection() throws Exception {
    createCollection(TARGET_COLLECTION);
  }

  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }

  protected CollectionAdminResponse createCollection(Map<String,List<Integer>> collectionInfos,
                                                     String collectionName, int numShards, int replicationFactor,
                                                     int maxShardsPerNode, SolrServer client, String createNodeSetStr)
  throws SolrServerException, IOException {
    return createCollection(collectionInfos, collectionName,
        ZkNodeProps.makeMap(
            NUM_SLICES, numShards,
            REPLICATION_FACTOR, replicationFactor,
            CREATE_NODE_SET, createNodeSetStr,
            MAX_SHARDS_PER_NODE, maxShardsPerNode),
        client);
  }

  protected CollectionAdminResponse deleteCollection(String collectionName) throws SolrServerException, IOException {
    SolrServer client = createCloudClient(null);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.DELETE.toString());
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse(client.request(request));

    client.shutdown();

    return res;
  }

  protected CloudSolrServer getSourceClient() {
    return createCloudClient(SOURCE_COLLECTION);
  }

  protected CloudSolrServer getTargetClient() {
    return createCloudClient(TARGET_COLLECTION);
  }

  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }

  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    CloudSolrServer client = this.getSourceClient();
    try {
      client.add(doc);
    }
    finally {
      client.shutdown();
    }
  }

  protected void commit(String collection) throws IOException, SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      client.commit();
    }
    finally {
      client.shutdown();
    }
  }

  protected long getNumDocs(String collection) throws SolrServerException {
    CloudSolrServer client = createCloudClient(collection);
    try {
      return client.query(new SolrQuery("*:*")).getResults().getNumFound();
    }
    finally {
      client.shutdown();
    }
  }

  protected String getLeaderUrl(String collection, String shardId) throws Exception {
    CloudSolrServer client = this.getSourceClient();
    try {
      client.connect(); // ensure that we are connected to the zookeeper ensemble
      ZkStateReader reader = client.getZkStateReader();
      return reader.getLeaderUrl(collection, shardId, AbstractZkTestCase.TIMEOUT);
    }
    finally {
      client.shutdown();
    }
  }

  protected List<String> getReplicaUrls(String collection, String shardId) {
    CloudSolrServer client = this.getSourceClient();
    try {
      client.connect(); // ensure that we are connected to the zookeeper ensemble
      ZkStateReader reader = client.getZkStateReader();
      ClusterState clusterState = reader.getClusterState();

      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      if (slices == null) {
        throw new ZooKeeperException(SolrException.ErrorCode.BAD_REQUEST,
            "Could not find collection in zk: " + collection + " " + clusterState.getCollections());
      }

      Slice replicas = slices.get(shardId);
      if (replicas == null) {
        throw new ZooKeeperException(SolrException.ErrorCode.BAD_REQUEST, "Could not find shardId in zk: " + shardId);
      }

      Map<String,Replica> shardMap = replicas.getReplicasMap();
      List<String> urls = new ArrayList<>(shardMap.size());
      for (Replica replica : shardMap.values()) {
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(replica);
        urls.add(nodeProps.getBaseUrl());
      }

      return urls;
    }
    finally {
      client.shutdown();
    }
  }

  /**
   * Creates a non-smart client to a replica (non-leader).
   */
  protected SolrServer getReplicaClient(String collection, String shard) throws Exception {
    List<String> replicas = getReplicaUrls(collection, shard);
    String leader = getLeaderUrl(collection, shard);
    /*
    getLeader returns e.g. http://127.0.0.1:59803/b_k/myCollection_shard1_replica1/
    while getReplicaUrls returns e.g. http://127.0.0.1:59803/b_k

     */
    String tmp = leader.substring(0, leader.length()-1);
    leader = tmp.substring(0, tmp.lastIndexOf('/'));

    replicas.remove(leader);
    return createNewSolrServer(collection, replicas.get(0));
  }

  /**
   * Creates a non-smart client to a leader.
   */
  protected SolrServer getLeaderClient(String collection, String shard) throws Exception {
    String leader = getLeaderUrl(collection, shard);
    /*
    getLeader returns e.g. http://127.0.0.1:59803/b_k/myCollection_shard1_replica1/
    while getReplicaUrls returns e.g. http://127.0.0.1:59803/b_k

     */
    String tmp = leader.substring(0, leader.length()-1);
    leader = tmp.substring(0, tmp.lastIndexOf('/'));

    return createNewSolrServer(collection, leader);
  }

  protected NamedList sendRequest(String baseUrl, CdcrRequestHandler.CdcrAction action) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, action.toString());

    SolrRequest request = new QueryRequest(params);
    request.setPath(CDCR_PATH);

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    return baseServer.request(request);
  }

}
