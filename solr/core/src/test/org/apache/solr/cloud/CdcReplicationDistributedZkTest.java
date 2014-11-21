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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.CdcrParams;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.Before;

public class CdcReplicationDistributedZkTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    this.doTestDeleteCreateSourceCollection();
    // this.doTestTargetCollectionNotAvailable();
    this.doTestReplicationStartStop();
    this.doTestReplicationAfterRestart();
    this.doTestReplicationAfterLeaderChange();
    this.doTestUpdateLogSynchronisation();
    this.doTestBufferOnNonLeader();
    this.doTestBatchAddsWithDelete();
  }

  /**
   * Checks that the test framework handles properly the creation and deletion of collections and the
   * restart of servers.
   */
  public void doTestDeleteCreateSourceCollection() throws Exception {
    log.info("Indexing documents");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);
    index(TARGET_COLLECTION, docs);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting leader @ source_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Clearing source_collection");

    this.clearSourceCollection();

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting leader @ target_collection:shard1");

    this.restartServer(shardToLeaderJetty.get(TARGET_COLLECTION).get(SHARD1));

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Clearing target_collection");

    this.clearTargetCollection();

    assertEquals(0, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    assertCollectionExpectations(SOURCE_COLLECTION);
    assertCollectionExpectations(TARGET_COLLECTION);
  }

  public void doTestTargetCollectionNotAvailable() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    this.printLayout(); // debug

    indexDoc(getDoc(id, "a"));
    indexDoc(getDoc(id, "b"));
    indexDoc(getDoc(id, "c"));
    indexDoc(getDoc(id, "d"));
    indexDoc(getDoc(id, "e"));
    indexDoc(getDoc(id, "f"));
    commit(SOURCE_COLLECTION);

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));

    // send start action to first shard
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);
    NamedList status = (NamedList) rsp.get(CdcrParams.CdcrAction.STATUS.toLower());
    assertEquals(CdcrParams.ProcessState.STARTED.toLower(), status.get(CdcrParams.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrParams.ProcessState.STARTED, CdcrParams.BufferState.ENABLED);

    // TODO: check error status when monitoring api is available
  }

  public void doTestReplicationStartStop() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the replication manager is properly restarted after a node failure.
   */
  public void doTestReplicationAfterRestart() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting shard1");

    this.restartServers(shardToJetty.get(SOURCE_COLLECTION).get(SHARD1));

    log.info("Indexing 100 documents");

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the replication manager is properly started after a change of leader.
   * This test also checks that the log readers on the new leaders are initialised with
   * the target's checkpoint.
   */
  public void doTestReplicationAfterLeaderChange() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 10; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    log.info("Restarting leaders");

    // Close all the leaders, then restart them
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2));

    log.info("Checking queue size of new leaders");

    // If the log readers of the new leaders are initialised with the target's checkpoint, the
    // queue size must be inferior to the current number of documents indexed.
    assertTrue(this.getQueueSize(SOURCE_COLLECTION, SHARD1) < 10);
    assertTrue(this.getQueueSize(SOURCE_COLLECTION, SHARD2) < 10);

    log.info("Indexing 100 documents");

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(110, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that the update logs are synchronised between leader and non-leader nodes
   */
  public void doTestUpdateLogSynchronisation() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // buffering is enabled by default, so disable it
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    for (int i = 0; i < 50; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i))); // will perform a commit for every document
    }

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));

    Thread.sleep(2000); // wait for the update log synchronisation to complete

    // logs on replicas should be trimmed
    assertUpdateLogs(SOURCE_COLLECTION, true);

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.STOP);

    for (int i = 50; i < 100; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i)));
    }

    Thread.sleep(2000); // wait for the update log synchronisation to complete

    // at this stage, we should have created one tlog file per document, and some of them must have been cleaned on the
    // leader since we are not buffering and replication is stopped
    // the non-leader must have synchronised its update log with its leader
    assertUpdateLogs(SOURCE_COLLECTION, true);
  }

  /**
   * Check that the buffer is always activated on non-leader nodes.
   */
  public void doTestBufferOnNonLeader() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // buffering is enabled by default, so disable it
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.DISABLEBUFFER);

    // Index documents
    for (int i = 0; i < 50; i++) {
      index(SOURCE_COLLECTION, getDoc(id, Integer.toString(i))); // will perform a commit for every document
    }

    // Close all the leaders, then restart them. At this stage, the new leader must have been elected
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2));

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertEquals(50, getNumDocs(SOURCE_COLLECTION));
    assertEquals(50, getNumDocs(TARGET_COLLECTION));
  }

  /**
   * Check that batch updates with deletes
   */
  public void doTestBatchAddsWithDelete() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    // Index 50 documents
    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 50; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Delete 10 documents: 10-19
    List<String> ids = new ArrayList<>();
    for (int id = 10; id < 20; id++) {
      ids.add(Integer.toString(id));
    }
    deleteById(SOURCE_COLLECTION, ids);

    // Index 10 documents
    docs = new ArrayList<>();
    for (; start < 60; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Delete 1 document: 50
    ids = new ArrayList<>();
    ids.add(Integer.toString(50));
    deleteById(SOURCE_COLLECTION, ids);

    // Index 10 documents
    docs = new ArrayList<>();
    for (; start < 70; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    // Start CDCR
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    // wait a bit for the replication to complete
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);
    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD2);

    commit(TARGET_COLLECTION);

    // If the non-leader node were buffering updates, then the replication must be complete
    assertEquals(59, getNumDocs(SOURCE_COLLECTION));
    assertEquals(59, getNumDocs(TARGET_COLLECTION));
  }

  protected void waitForReplicationToComplete(String collectionName, String shardId) throws Exception {
    while (true) {
      log.info("Checking queue size @ {}:{}", collectionName, shardId);
      long size = this.getQueueSize(collectionName, shardId);
      if (size <= 0) {
        return;
      }
      log.info("Waiting for replication to complete. Queue size: {} @ {}:{}", size, collectionName, shardId);
      Thread.sleep(1000); // wait a bit for the replication to complete
    }
  }

  protected long getQueueSize(String collectionName, String shardId) throws Exception {
    NamedList rsp = this.invokeCdcrAction(shardToLeaderJetty.get(collectionName).get(shardId), CdcrParams.CdcrAction.QUEUESIZE);
    NamedList status = (NamedList) rsp.get(CdcrParams.QUEUES);
    return (Long) status.get(TARGET_COLLECTION);
  }

  /**
   * if equals == true then checks that update logs contain the same number of files
   * otherwise asserts the numbers are different
   */
  protected void assertUpdateLogs(String collection, boolean equals) throws Exception {
    CollectionInfo info = collectInfo(collection);
    Map<String, List<CollectionInfo.CoreInfo>> shardToCoresMap = info.getShardToCoresMap();
    for (String shard : shardToCoresMap.keySet()) {
      CollectionInfo.CoreInfo leader = info.getLeader(shard);
      List<CollectionInfo.CoreInfo> replicas = info.getReplicas(shard);
      assertUpdateLogs(leader, replicas, equals);
    }
  }

  private void assertUpdateLogs(CollectionInfo.CoreInfo leader, List<CollectionInfo.CoreInfo> replicas, boolean equals) {
    int leaderLogs = numberOfFiles(leader.ulogDir);

    for (CollectionInfo.CoreInfo replica : replicas) {
      int replicaLogs = numberOfFiles(replica.ulogDir);
      log.info("Number of logs in update log on leader {} {} {} and on replica {} {} {}",
          leader.shard, leader.collectionName, leaderLogs, replica.shard, replica.collectionName, replicaLogs);
      if (equals) {
        assertEquals(String.format(Locale.ENGLISH,"Number of tlogs on replica %s %s: %d is different than on leader: %d.",
                replica.collectionName, replica.shard, replicaLogs, leaderLogs),
            leaderLogs, replicaLogs);
      } else {
        assertTrue(String.format(Locale.ENGLISH,"Number of tlogs on replica %s %s: %d is the same as on leader: %d.",
                replica.collectionName, replica.shard, replicaLogs, leaderLogs),
            leaderLogs != replicaLogs);
      }
    }
  }

  private int numberOfFiles(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }
    log.info("Update log dir {} contains: {}", dir, file.listFiles());
    return file.listFiles().length;
  }

  private CollectionInfo collectInfo(String collection) throws Exception {
    CollectionInfo info = new CollectionInfo(collection);
    for (String shard : shardToJetty.get(collection).keySet()) {
      List<CloudJettyRunner> jettyRunners = shardToJetty.get(collection).get(shard);
      for (CloudJettyRunner jettyRunner : jettyRunners) {
        SolrDispatchFilter filter = (SolrDispatchFilter)jettyRunner.jetty.getDispatchFilter().getFilter();
        for (SolrCore core : filter.getCores().getCores()) {
          info.addCore(core, shard, shardToLeaderJetty.get(collection).containsValue(jettyRunner));
        }
      }
    }

    return info;
  }

  private class CollectionInfo {

    List<CoreInfo> coreInfos = new ArrayList<>();

    String collection;

    CollectionInfo(String collection) {
      this.collection = collection;
    }

    /**
     * @return Returns a map shard -> list of cores
     */
    Map<String, List<CoreInfo>> getShardToCoresMap() {
      Map<String, List<CoreInfo>> map = new HashMap<>();
      for (CoreInfo info : coreInfos) {
        List<CoreInfo> list = map.get(info.shard);
        if (list == null) {
          list = new ArrayList<>();
          map.put(info.shard, list);
        }
        list.add(info);
      }
      return map;
    }

    CoreInfo getLeader(String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap().get(shard);
      for (CoreInfo info : coreInfos) {
        if (info.isLeader) {
          return info;
        }
      }
      assertTrue(String.format(Locale.ENGLISH,"There is no leader for collection %s shard %s", collection, shard), false);
      return null;
    }

    List<CoreInfo> getReplicas(String shard) {
      List<CoreInfo> coreInfos = getShardToCoresMap().get(shard);
      coreInfos.remove(getLeader(shard));
      return coreInfos;
    }

    void addCore(SolrCore core, String shard, boolean isLeader) throws Exception {
      CoreInfo info = new CoreInfo();
      info.collectionName = core.getName();
      info.shard = shard;
      info.isLeader = isLeader;
      info.ulogDir = core.getUlogDir() + "tlog";

      this.coreInfos.add(info);
    }

    public class CoreInfo {
      String collectionName;
      String shard;
      boolean isLeader;
      String ulogDir;
    }

  }

}
