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

import org.apache.solr.handler.CdcrRequestHandler;
import org.junit.Before;

public class CdcrAPIDistributedZkTest extends AbstractCdcrDistributedZkTest {

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

    indexDoc(getDoc(id, "a"));
    indexDoc(getDoc(id, "b"));
    indexDoc(getDoc(id, "c"));
    indexDoc(getDoc(id, "d"));
    indexDoc(getDoc(id, "e"));
    indexDoc(getDoc(id, "f"));
    commit(SOURCE_COLLECTION);

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));
    assertEquals(0, getNumDocs(TARGET_COLLECTION));

    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);

    Thread.sleep(1000); // wait a bit for the replication to complete

    commit(TARGET_COLLECTION);

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));
    assertEquals(6, getNumDocs(TARGET_COLLECTION));

    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);

    int start = 0;
    for (int i = start; i < start + 100; i++) {
      indexDoc(getDoc(id, Integer.toString(i)));
    }
    commit(SOURCE_COLLECTION);

    assertEquals(106, getNumDocs(SOURCE_COLLECTION));
    assertEquals(6, getNumDocs(TARGET_COLLECTION));

    // Start again CDCR, the source cluster should reinitialise its log readers
    // with the latest checkpoints

    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);
    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);

    Thread.sleep(2000); // wait a bit for the replication to complete

    commit(TARGET_COLLECTION);

    assertEquals(106, getNumDocs(SOURCE_COLLECTION));
    assertEquals(106, getNumDocs(TARGET_COLLECTION));

    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
    this.sendRequest(getLeaderUrl(TARGET_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.STOP);
  }

}
