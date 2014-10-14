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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.CdcrRequestHandler;
import org.junit.Before;

public class CdcrRequestHandlerTest extends BasicDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    fixShardCount = true;
    sliceCount = 2;
    shardCount = 2;
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
//    this.doTestLifeCycleActions();
    this.doTestCheckpointActions();
  }

  // check that the life-cycle state is properly synchronised across nodes
  public void doTestLifeCycleActions(){
    try {
      // check initial status
      this.assertState(CdcrRequestHandler.CdcrState.STOPPED);

      // send start action
      NamedList rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.START);
      String status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
      assertEquals(CdcrRequestHandler.CdcrState.STARTED.toLower(), status);

      // check status
      this.assertState(CdcrRequestHandler.CdcrState.STARTED);

      // send stop action
      rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.STOP);
      status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
      assertEquals(CdcrRequestHandler.CdcrState.STOPPED.toLower(), status);

      // check status
      this.assertState(CdcrRequestHandler.CdcrState.STOPPED);
    }
    catch (SolrServerException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void assertState(CdcrRequestHandler.CdcrState state) throws IOException, SolrServerException {
    NamedList rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.STATUS);
    String status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(state.toLower(), status);

    rsp = sendRequest(SHARD2, CdcrRequestHandler.CdcrAction.STATUS);
    status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(state.toLower(), status);
  }

  // check
  public void doTestCheckpointActions() throws Exception {
    try {
      // initial request on an empty index, must return -1
      NamedList rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
      assertEquals(-1l, rsp.get("checkpoint"));

      index(id, "a"); // shard 2
      commit();

      // only one document indexed in shard 2, the checkpoint must be still -1
      rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
      assertEquals(-1l, rsp.get("checkpoint"));

      index(id, "b"); // shard 1
      commit();

      // a second document indexed in shard 1, the checkpoint must come from shard 2
      rsp = sendRequest(SHARD2, CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
      long checkpoint1 = (Long) rsp.get("checkpoint");
      long expected = (Long) sendRequest(SHARD2, CdcrRequestHandler.CdcrAction.SLICECHECKPOINT).get("checkpoint");
      assertEquals(expected, checkpoint1);

      index(id, "c"); // shard 1
      commit();

      // a third document indexed in shard 1, the checkpoint must still come from shard 2
      rsp = sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
      assertEquals(checkpoint1, rsp.get("checkpoint"));

      index(id, "d"); // shard 2
      commit();

      // a fourth document indexed in shard 2, the checkpoint must come from shard 1
      rsp = sendRequest(SHARD2, CdcrRequestHandler.CdcrAction.COLLECTIONCHECKPOINT);
      long checkpoint2 = (Long) rsp.get("checkpoint");
      expected = (Long) sendRequest(SHARD1, CdcrRequestHandler.CdcrAction.SLICECHECKPOINT).get("checkpoint");
      assertEquals(expected, checkpoint2);
    }
    catch (SolrServerException e) {
      e.printStackTrace();
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected NamedList sendRequest(String shardId, CdcrRequestHandler.CdcrAction action)
  throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, action.toString());

    SolrRequest request = new QueryRequest(params);
    request.setPath("/cdcr");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(shardId).get(0).client.solrClient).getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    return baseServer.request(request);
  }

}
