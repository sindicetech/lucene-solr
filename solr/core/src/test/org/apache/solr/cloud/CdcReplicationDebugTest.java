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
import java.util.Collections;
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

public class CdcReplicationDebugTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    this.doTestReplicationAfterLeaderChange();
  }

  /**
   * Check that the replication manager is properly started after a change of leader.
   * This test also checks that the log readers on the new leaders are initialised with
   * the target's checkpoint.
   */
  public void doTestReplicationAfterLeaderChange() throws Exception {
    this.clearSourceCollection();
    this.clearTargetCollection();

    log.info("Indexing 10 documents");

    int start = 0;
    List<SolrInputDocument> docs = new ArrayList<>();
    for (; start < 128; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    log.info("Querying source collection");

    assertEquals(128, getNumDocs(SOURCE_COLLECTION));

    log.info("Restarting source leaders");

    // Close all the leaders, then restart them
    // this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));
    ChaosMonkey.stop(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1).jetty);
    updateMappingsFromZk(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1).collection);

    log.info("Starting CDCR");

    // send start action to first shard
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrParams.CdcrAction.START);

    log.info("Waiting for replication");

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    log.info("Querying target collection");

    commit(TARGET_COLLECTION);
    assertEquals(128, getNumDocs(TARGET_COLLECTION));
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
    NamedList rsp = this.invokeCdcrAction(shardToLeaderJetty.get(collectionName).get(shardId), CdcrParams.CdcrAction.QUEUES);
    NamedList status = (NamedList) ((NamedList) rsp.get(CdcrParams.QUEUES)).getVal(0);
    return (Long) status.get(CdcrParams.QUEUE_SIZE);
  }

  /**
   * Asserts that the number of transaction logs across all the shards
   */
  protected void assertUpdateLogs(String collection, int maxNumberOfTLogs) throws Exception {
    CollectionInfo info = collectInfo(collection);
    Map<String, List<CollectionInfo.CoreInfo>> shardToCoresMap = info.getShardToCoresMap();

    int leaderLogs = 0;
    ArrayList<Integer> replicasLogs = new ArrayList<>(Collections.nCopies(replicationFactor - 1, 0));

    for (String shard : shardToCoresMap.keySet()) {
      leaderLogs += numberOfFiles(info.getLeader(shard).ulogDir);
      for (int i = 0; i < replicationFactor - 1; i++) {
        replicasLogs.set(i, replicasLogs.get(i) + numberOfFiles(info.getReplicas(shard).get(i).ulogDir));
      }
    }

    for (Integer replicaLogs : replicasLogs) {
      log.info("Number of logs in update log on leader {} and on replica {}", leaderLogs, replicaLogs);

      // replica logs must be always equal or superior to leader logs
      assertTrue(String.format(Locale.ENGLISH, "Number of tlogs on replica: %d is different than on leader: %d.",
          replicaLogs, leaderLogs), leaderLogs <= replicaLogs);

      assertTrue(String.format(Locale.ENGLISH, "Number of tlogs on leader: %d is superior to: %d.",
          leaderLogs, maxNumberOfTLogs), maxNumberOfTLogs > leaderLogs);

      assertTrue(String.format(Locale.ENGLISH, "Number of tlogs on replica: %d is superior to: %d.",
          replicaLogs, maxNumberOfTLogs), maxNumberOfTLogs > replicaLogs);
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
      info.ulogDir = core.getUpdateHandler().getUpdateLog().getLogDir();

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
