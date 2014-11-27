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
    // this.createTargetCollection();
    this.printLayout(); // debug

    // placeholder for future tests
    indexDoc(getDoc(id, "a"));
    indexDoc(getDoc(id, "b"));
    indexDoc(getDoc(id, "c"));
    indexDoc(getDoc(id, "d"));
    indexDoc(getDoc(id, "e"));
    indexDoc(getDoc(id, "f"));
    commit();

    assertEquals(6, getNumDocs(SOURCE_COLLECTION));
    // assertEquals(0, getNumDocs(TARGET_COLLECTION));

    this.sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.START);

    log.info("Sleeping 1s");
    Thread.currentThread().sleep(1000);
    log.info("Finished sleeping");

    CloudJettyRunner runner = shardToLeaderJetty.get(SHARD1);
    log.info("Stopping leader of shard1 - {}", runner.coreNodeName);
    ChaosMonkey.stop(runner.jetty);

    log.info("Sleeping 2s");
    Thread.currentThread().sleep(2000);
    log.info("Finished sleeping");

    log.info("Bringing back up the node");
    ChaosMonkey.start(runner.jetty);
  }

}
