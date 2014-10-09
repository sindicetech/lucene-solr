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
    fixShardCount = true;
    sliceCount = 2;
    shardCount = 2;
    super.setUp();
  }

  /**
   * Perform the actual tests here
   *
   * @throws Exception on error
   */
  @Override
  public void doTest() throws Exception {
    this.doTestLifeCycleActions();
  }

  public void doTestLifeCycleActions(){
    try {

      // check initial status
      this.assertState(CdcrRequestHandler.CdcrState.STOPPED);

      // send start action
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.ACTION, CdcrRequestHandler.CdcrAction.START.toString());

      NamedList rsp = sendRequest(SHARD1, params);
      String status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
      assertEquals(CdcrRequestHandler.CdcrState.STARTED.toLower(), status);

      // check status
      this.assertState(CdcrRequestHandler.CdcrState.STARTED);

      // send stop action
      params = new ModifiableSolrParams();
      params.set(CommonParams.ACTION, CdcrRequestHandler.CdcrAction.STOP.toString());

      rsp = sendRequest(SHARD1, params);
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
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, CdcrRequestHandler.CdcrAction.STATUS.toString());

    NamedList rsp = sendRequest(SHARD1, params);
    String status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(state.toLower(), status);

    rsp = sendRequest(SHARD2, params);
    status = (String) rsp.get(CdcrRequestHandler.CdcrAction.STATUS.toLower());
    assertEquals(state.toLower(), status);
  }

  protected NamedList sendRequest(String shardId, ModifiableSolrParams params) throws SolrServerException, IOException {
    SolrRequest request = new QueryRequest(params);
    request.setPath("/cdcr");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(shardId).get(0).client.solrClient).getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    return baseServer.request(request);
  }

}
