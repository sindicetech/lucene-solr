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

import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrRequestHandler;
import org.junit.Before;

public class CdcrRequestHandlerTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    createTargetCollection = false;     // we do not need the target cluster
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    //this.doTestLifeCycleActions();
    this.doTestCheckpointActions();
    //this.doTestBufferActions();
  }

  // check that the life-cycle state is properly synchronised across nodes
  public void doTestLifeCycleActions() throws Exception {
    // check initial status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.ENABLED);

    // send start action to first shard
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.START);
    NamedList status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(CdcrRequestHandler.ProcessState.STARTED.toLower(), status.get(CdcrRequestHandler.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STARTED, CdcrRequestHandler.BufferState.ENABLED);

    // Restart the leader of shard 1
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    // check status - the node that died should have picked up the original state
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STARTED, CdcrRequestHandler.BufferState.ENABLED);

    // send stop action to second shard
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrRequestHandler.CdcrAction.STOP);
    status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(CdcrRequestHandler.ProcessState.STOPPED.toLower(), status.get(CdcrRequestHandler.ProcessState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.ENABLED);
  }

  // check the checkpoint API
  public void doTestCheckpointActions() throws Exception {
    // initial request on an empty index, must return -1
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(-1l, rsp.get("checkpoint"));

    index(SOURCE_COLLECTION, getDoc(id, "a")); // shard 2

    // only one document indexed in shard 2, the checkpoint must be still -1
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(-1l, rsp.get("checkpoint"));

    index(SOURCE_COLLECTION, getDoc(id, "b")); // shard 1

    // a second document indexed in shard 1, the checkpoint must come from shard 2
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint1 = (Long) rsp.get("checkpoint");
    long expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrRequestHandler.CdcrAction.SLICECHECKPOINT).get("checkpoint");
    assertEquals(expected, checkpoint1);

    index(SOURCE_COLLECTION, getDoc(id, "c")); // shard 1

    // a third document indexed in shard 1, the checkpoint must still come from shard 2
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
    assertEquals(checkpoint1, rsp.get("checkpoint"));

    index(SOURCE_COLLECTION, getDoc(id, "d")); // shard 2

    // a fourth document indexed in shard 2, the checkpoint must come from shard 1
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
    long checkpoint2 = (Long) rsp.get("checkpoint");
    expected = (Long) invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.SLICECHECKPOINT).get("checkpoint");
    assertEquals(expected, checkpoint2);

    // replication never started, lastProcessedVersion should be 0 for both shards
    rsp = sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD1), CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION);
    long lastVersion = (Long) rsp.get("lastProcessedVersion");
    assertEquals(0, lastVersion);

    rsp = sendRequest(getLeaderUrl(SOURCE_COLLECTION, SHARD2), CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION);
    lastVersion = (Long) rsp.get("lastProcessedVersion");
    assertEquals(0, lastVersion);
  }

  // check that the buffer state is properly synchronised across nodes
  public void doTestBufferActions() throws Exception {
    // check initial status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.ENABLED);

    // send disable buffer action to first shard
    NamedList rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1), CdcrRequestHandler.CdcrAction.DISABLEBUFFER);
    NamedList status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(CdcrRequestHandler.BufferState.DISABLED.toLower(), status.get(CdcrRequestHandler.BufferState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.DISABLED);

    // Restart the leader of shard 1
    this.restartServer(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD1));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.DISABLED);

    // send enable buffer action to second shard
    rsp = invokeCdcrAction(shardToLeaderJetty.get(SOURCE_COLLECTION).get(SHARD2), CdcrRequestHandler.CdcrAction.ENABLEBUFFER);
    status = (NamedList) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(CdcrRequestHandler.BufferState.ENABLED.toLower(), status.get(CdcrRequestHandler.BufferState.getParam()));

    // check status
    this.assertState(SOURCE_COLLECTION, CdcrRequestHandler.ProcessState.STOPPED, CdcrRequestHandler.BufferState.ENABLED);
  }

}
