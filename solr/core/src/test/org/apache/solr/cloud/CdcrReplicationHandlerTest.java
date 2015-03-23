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

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;

public class CdcrReplicationHandlerTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    createTargetCollection = false;     // we do not need the target cluster
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    // this.doTestFullReplication();
    this.doTestPartialReplication();
  }

  public void doTestFullReplication() throws Exception {
    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    ChaosMonkey.stop(slaves.get(0).jetty);

    for (int i = 0; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 100; j < (i * 100) + 100; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertEquals(1000, getNumDocs(SOURCE_COLLECTION));

    this.restartServer(slaves.get(0));

    assertEquals(1000, getNumDocs(SOURCE_COLLECTION));

    this.assertUpdateLogs(SOURCE_COLLECTION, 10);
  }

  public void doTestPartialReplication() throws Exception {
    for (int i = 0; i < 5; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 100; j < (i * 100) + 100; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    List<CloudJettyRunner> slaves = this.getShardToSlaveJetty(SOURCE_COLLECTION, SHARD1);
    ChaosMonkey.stop(slaves.get(0).jetty);

    for (int i = 5; i < 10; i++) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int j = i * 100; j < (i * 100) + 100; j++) {
        docs.add(getDoc(id, Integer.toString(j)));
      }
      index(SOURCE_COLLECTION, docs);
    }

    assertEquals(1000, getNumDocs(SOURCE_COLLECTION));

    this.restartServer(slaves.get(0));

    assertEquals(1000, getNumDocs(SOURCE_COLLECTION));

    this.assertUpdateLogs(SOURCE_COLLECTION, 10);
  }

  private List<CloudJettyRunner> getShardToSlaveJetty(String collection, String shard) {
    List<CloudJettyRunner> jetties = new ArrayList<>(shardToJetty.get(collection).get(shard));
    CloudJettyRunner leader = shardToLeaderJetty.get(collection).get(shard);
    jetties.remove(leader);
    return jetties;
  }

}