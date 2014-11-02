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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrRequestHandler;
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
    // this.doTestDeleteCreateSourceCollection();
    // this.doTestTargetCollectionNotAvailable();
    // this.doTestReplicationAfterRestart();
    // this.doTestReplicationAfterLeaderChange();
    this.doTestReplicationStartStop();
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
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);
    NamedList status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(CdcrRequestHandler.ProcessState.STARTED.toLower(), status.get(CdcrRequestHandler.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STARTED, CdcrRequestHandler.BufferState.ENABLED);

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

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);

    this.waitForReplicationToComplete(SOURCE_COLLECTION, SHARD1);

    commit(TARGET_COLLECTION);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.STOP);

    docs.clear();
    for (; start < 110; start++) {
      docs.add(getDoc(id, Integer.toString(start)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(110, getNumDocs(SOURCE_COLLECTION));
    assertEquals(10, getNumDocs(TARGET_COLLECTION));

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);

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
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);

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
    this.invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);

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
    NamedList rsp = this.invokeCdcrAction(shardToLeaderJetty.get(collectionName).get(shardId), CdcrRequestHandler.CdcrAction.QUEUESIZE);
    NamedList status = (NamedList) rsp.get("queue");
    return (Long) status.get(TARGET_COLLECTION);
  }

}
