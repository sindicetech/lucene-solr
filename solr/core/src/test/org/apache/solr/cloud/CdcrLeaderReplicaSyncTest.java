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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.CdcrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.RefCounted;
import org.junit.Before;

public class CdcrLeaderReplicaSyncTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    this.createTargetCollection();
    this.printLayout(); // debug

    // buffering is enabled by default, so disable it
    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.DISABLEBUFFER);

    indexDoc(getDoc(id, "a"));//shard2
    indexDoc(getDoc(id, "b"));//shard1
    commit(SOURCE_COLLECTION);

    assertEquals(2, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    // wait a bit for the replication to complete
    // CdcrNonLeaderScheduler is not running since replication is stopped
    Thread.sleep(2000);

    ClusterInfo info = collectClusterInfo();
    log.info(info.toString());

    assertUpdateLogsOk(info, SOURCE_COLLECTION, true);

    //TODO: cdc replication is stopped and buffering enabled makes no sense...?

    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.ENABLEBUFFER);

    indexDoc(getDoc(id, "c"));
    indexDoc(getDoc(id, "d"));
    commit(SOURCE_COLLECTION);

    indexDoc(getDoc(id, "e"));
    indexDoc(getDoc(id, "f"));
    commit(SOURCE_COLLECTION);

    // wait a bit for the replication to complete
    // and to give CdcrNonLeaderScheduler a chance to trim update logs on replicas, which it shouldn't now
    Thread.sleep(2000);

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    info = collectClusterInfo();
    log.info(info.toString());

    // buffering is enabled so logs on replicas should not be trimmed
    assertUpdateLogsOk(info, SOURCE_COLLECTION, false);

//
//    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
//    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
//
//    Thread.sleep(1000); // wait a bit for the replication to complete
//
//    commit(TARGET_COLLECTION);
//
//    rsp = sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD2), CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION);
//    lastVersion = (Long) rsp.get("lastProcessedVersion");
//    // buffering is enabled by default, so lastVersion must be 0
//    assertEquals(0, lastVersion);
//
//    assertEquals(6, getNumDocs(SOURCE_COLLECTION));
//    assertEquals(6, getNumDocs(TARGET_COLLECTION));
//
//    // disable buffering
//    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.DISABLEBUFFER);
//
//    queryResponse = cloudClient.query(params("qt","/get", "ids", "f"));
//    expectedVersion = (long) queryResponse.getResults().get(0).get("_version_");
//
//    rsp = sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD2), CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION);
//    lastVersion = (Long) rsp.get("lastProcessedVersion");
//    assertEquals(expectedVersion, lastVersion);
//
//
//
//    List<String> replicaUrls = getReplicaUrls(SOURCE_COLLECTION, SHARD2);
//    String leader = getTruncatedLeaderUrl(SOURCE_COLLECTION, SHARD2);
//    assertTrue(replicaUrls.remove(leader));
//
//    rsp = sendRequest(replicaUrls.get(0), CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION);
//    lastVersion = (Long) rsp.get("lastProcessedVersion");
//    System.out.println("Replica lastProcessedVersion = " + lastVersion);
//    assertEquals(expectedVersion, lastVersion);

//    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
//    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
//
//    int start = 0;
//    for (int i = start; i < start + 100; i++) {
//      indexDoc(getDoc(id, Integer.toString(i)));
//    }
//    commit(SOURCE_COLLECTION);
//
//    assertEquals(106, getNumDocs(SOURCE_COLLECTION));
//    assertEquals(6, getNumDocs(TARGET_COLLECTION));
//
//    // Start again CDCR, the source cluster should reinitialise its log readers
//    // with the latest checkpoints
//
//    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
//    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
//
//    Thread.sleep(2000); // wait a bit for the replication to complete
//
//    commit(TARGET_COLLECTION);
//
//    assertEquals(106, getNumDocs(SOURCE_COLLECTION));
//    assertEquals(106, getNumDocs(TARGET_COLLECTION));
//
//    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
//    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
  }

  /**
   *
   * if equals == true then checks that update logs contain the same number of files
   * otherwise asserts the numbers are different
   */
  void assertUpdateLogsOk(ClusterInfo info, String collection, boolean equals) {
    Map<String, List<CoreInfo>> shardToCoresMap = info.getShardToCoresMap(collection);
    for (String shard : shardToCoresMap.keySet()) {
      CoreInfo leader = info.getLeader(collection, shard);
      List<CoreInfo> replicas = info.getReplicas(collection, shard);
      assertUpdateLogsOk(leader, replicas, equals);
    }
  }

  int numberOfFiles(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }
    return file.listFiles().length;
  }

  void assertUpdateLogsOk(CoreInfo leader, List<CoreInfo> replicas, boolean equals) {
    int leaderLogs = numberOfFiles(leader.ulogDir);

    for (CoreInfo replica : replicas) {
      int replicaLogs = numberOfFiles(replica.ulogDir);
      log.info("Number of logs in update log on leader {} {} {} and on replica {} {} {}",
          leader.shard, leader.collectionName, leaderLogs, replica.shard, replica.collectionName, replicaLogs);
      if (equals) {
        assertEquals(String.format("Number of tlogs on replica %s %s: %d is different than on leader: %d.",
                replica.collectionName, replica.shard, replicaLogs, leaderLogs),
            leaderLogs, replicaLogs);
      } else {
        assertTrue(String.format("Number of tlogs on replica %s %s: %d is the same as on leader: %d.",
                replica.collectionName, replica.shard, replicaLogs, leaderLogs),
            leaderLogs != replicaLogs);
      }
    }
  }

  List<CloudJettyRunner> getReplicaJettys(String shard) {
    List<CloudJettyRunner> jettys = new ArrayList(shardToJetty.get(shard));
    assertTrue(jettys.remove(shardToLeaderJetty.get(shard)));
    return jettys;
  }

  DocInfo collectDocInfo(StoredDocument doc) {
    DocInfo info = new DocInfo();
    info._version_ = doc.get("_version_");
    info.id = doc.get("id");
    return info;
  }

  CoreInfo collectCoreInfo(SolrCore core, String shard, boolean isLeader) throws Exception {
//    RefCounted<SolrIndexSearcher> ref = core.getSearcher();
//    SolrIndexSearcher searcher = ref.get();
//    SolrIndexSearcher.QueryCommand qc = new SolrIndexSearcher.QueryCommand();
//    qc.setQuery(new MatchAllDocsQuery());
//    SolrIndexSearcher.QueryResult queryResult = searcher.search(new SolrIndexSearcher.QueryResult(), qc);
//    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10);

    // searcher.maxDoc() can include deleted docs

    CoreInfo info = new CoreInfo();
    info.collectionName = core.getName();
    info.shard = shard;
    info.isLeader = isLeader;
    info.maxDoc = -1; //searcher.maxDoc();
    info.ulogDir = core.getUlogDir();


    Set<String> fields = new HashSet<>(); fields.add("id");
//    if (topDocs != null) {
//      for (ScoreDoc doc : topDocs.scoreDocs) {
//        StoredDocument sdoc = searcher.doc(doc.doc, fields);
//        info.topDocs.add(collectDocInfo(sdoc));
//      }
//    }
//
//    searcher.close();
//    ref.decref();

    return info;
  }

  boolean isLeader(JettySolrRunner solrRunner) {
    for (CloudJettyRunner cloudRunner : shardToLeaderJetty.values()) {
      if (cloudRunner.jetty.equals(solrRunner)) {
        return true;
      }
    }
    return false;
  }

  boolean isLeader(CloudJettyRunner jettyRunner) {
    return shardToLeaderJetty.containsValue(jettyRunner);
  }

  ClusterInfo collectClusterInfo() throws Exception {
    ClusterInfo info = new ClusterInfo();
    for (String shard : shardToJetty.keySet()) {
      List<CloudJettyRunner> jettyRunners = shardToJetty.get(shard);
      for (CloudJettyRunner jettyRunner : jettyRunners) {
        for (SolrCore core : getCores(jettyRunner.jetty)) {
          info.coreInfos.add(collectCoreInfo(core, shard, isLeader(jettyRunner.jetty)));
        }
      }
    }

    return info;
  }

  Collection<SolrCore> getCores(JettySolrRunner solrRunner) {
    return ((SolrDispatchFilter)solrRunner.getDispatchFilter().getFilter()).getCores().getCores();
  }

  class ClusterInfo {
    List<CoreInfo> coreInfos = new ArrayList<>();
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Cluster documents overview: \n");
      for (CoreInfo info : coreInfos) {
        sb.append("\t").append(info.toString()).append("\n");
      }
      return sb.toString();
    }

    // TODO: safer matching, could lead to false positives
    /*
    core name is sometimes:
         collection1
    and sometimes:
         target_collection_shard1_replica1
     */
    boolean match(String coreCollectionName, String collection) {
      return coreCollectionName.startsWith(collection);
    }

    /**
     * @return Returns a map shard -> list of cores for the given collection.
     */
    public Map<String, List<CoreInfo>> getShardToCoresMap(String collection) {
      Map<String, List<CoreInfo>> map = new HashMap<>();
      for (CoreInfo info : coreInfos) {
        if (match(info.collectionName, collection)) {
          List<CoreInfo> list = map.get(info.shard);
          if (list == null) {
            list = new ArrayList<>();
            map.put(info.shard, list);
          }
          list.add(info);
        }
      }
      return map;
    }

    public CoreInfo getLeader(String collection, String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap(collection).get(shard);
      for (CoreInfo info : coreInfos) {
        if (info.isLeader) {
          return info;
        }
      }
      assertTrue(String.format("There is no leader for collection %s shard %s", collection, shard), false);
      return null;
    }

    public List<CoreInfo> getReplicas(String collection, String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap(collection).get(shard);
      coreInfos.remove(getLeader(collection, shard));
      return coreInfos;
    }
  }

  class CoreInfo {
    String collectionName;
    String shard;
    boolean isLeader;
    int maxDoc;
    String ulogDir;
    List<DocInfo> topDocs = new ArrayList<>();
    public String toString() {
      return String.format("Shard %s %s %s contains max %d documents: %s", shard, collectionName, isLeader ? " (*)" : "    ", maxDoc, topDocs);
    }
  }

  class DocInfo {
    String id;
    String _version_;

    public String toString() {
      return String.format("%s(%s)", id, _version_);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DocInfo docInfo = (DocInfo) o;

      if (_version_ != null ? !_version_.equals(docInfo._version_) : docInfo._version_ != null) return false;
      if (id != null ? !id.equals(docInfo.id) : docInfo.id != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = id != null ? id.hashCode() : 0;
      result = 31 * result + (_version_ != null ? _version_.hashCode() : 0);
      return result;
    }
  }

}
