/**
 * Copyright (c) 2015 Renaud Delbru. All Rights Reserved.
 */
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
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;

public class CdcrReplicationHandlerTest extends CdcReplicationDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    createTargetCollection = false;     // we do not need the target cluster
    sliceCount = 1;                     // we need only one shard
    // we need a persistent directory, otherwise the UpdateHandler will erase existing tlog files after restarting a node
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    super.setUp();
  }

  /**
   * Perform the actual tests here
   *
   * @throws Exception on error
   */
  @Override
  public void doTest() throws Exception {
    this.doTestFullReplication();
  }

  /**
   * Test the scenario where the slave is killed from the start. The replication
   * strategy should fetch all the missing tlog files from the leader.
   */
  public void doTestFullReplication() throws Exception {
    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    ChaosMonkey.stop(slaves.get(0).jetty);

    for (int i = 0; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
      for (int j = i * 10; j < (i * 10) + 10; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertEquals(100, getNumDocs(SOURCE_COLLECTION));

    // Restart the slave node to trigger Replication strategy
    this.restartServer(slaves.get(0));

    this.assertUpdateLogs(SOURCE_COLLECTION, 10);
  }

  private List<CloudJettyRunner> getShardToSlaveJetty(String collection, String shard) {
    List<CloudJettyRunner> jetties = new ArrayList<CloudJettyRunner>(shardToJetty.get(collection).get(shard));
    CloudJettyRunner leader = shardToLeaderJetty.get(collection).get(shard);
    jetties.remove(leader);
    return jetties;
  }

  /**
   * Asserts that the transaction logs between the leader and slave
   */
  @Override
  protected void assertUpdateLogs(String collection, int maxNumberOfTLogs) throws Exception {
    CollectionInfo info = collectInfo(collection);
    Map<String, List<CollectionInfo.CoreInfo>> shardToCoresMap = info.getShardToCoresMap();

    for (String shard : shardToCoresMap.keySet()) {
      Map<Long, Long> leaderFilesMeta = this.getFilesMeta(info.getLeader(shard).ulogDir);
      Map<Long, Long> slaveFilesMeta = this.getFilesMeta(info.getReplicas(shard).get(0).ulogDir);

      assertEquals("Incorrect number of tlog files on the leader", maxNumberOfTLogs, leaderFilesMeta.size());
      assertEquals("Incorrect number of tlog files on the slave", maxNumberOfTLogs, slaveFilesMeta.size());

      for (Long leaderFileVersion : leaderFilesMeta.keySet()) {
        assertTrue("Slave is missing a tlog for version " + leaderFileVersion, slaveFilesMeta.containsKey(leaderFileVersion));
        assertEquals("Slave's tlog file size differs for version " + leaderFileVersion, leaderFilesMeta.get(leaderFileVersion), slaveFilesMeta.get(leaderFileVersion));
      }
    }
  }

  private Map<Long, Long> getFilesMeta(String dir) {
    File file = new File(dir);
    if (!file.isDirectory()) {
      assertTrue("Path to tlog " + dir + " does not exists or it's not a directory.", false);
    }

    Map<Long, Long> filesMeta = new HashMap<Long, Long>();
    for (File tlogFile : file.listFiles()) {
      filesMeta.put(Math.abs(Long.parseLong(tlogFile.getName().substring(tlogFile.getName().lastIndexOf('.') + 1))), tlogFile.length());
    }
    return filesMeta;
  }

}
