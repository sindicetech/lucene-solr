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
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;

public class CdcrDebugRequestHandlerTest extends AbstractCdcrDistributedZkTest {

  @Override
  @Before
  public void setUp() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    createTargetCollection = false;     // we do not need the target cluster
    super.setUp();
  }

  @Override
  public void doTest() throws Exception {
    this.doTestLifeCycleActions();
  }

  // check that the life-cycle state is properly synchronised across nodes
  public void doTestLifeCycleActions() throws Exception {
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      docs.add(getDoc(id, Integer.toString(i)));
    }
    index(SOURCE_COLLECTION, docs);

    assertEquals(10, getNumDocs(SOURCE_COLLECTION));

    invokePullFromTo(shardToJetty.get(SOURCE_COLLECTION).get(SHARD1).get(0), shardToJetty.get(SOURCE_COLLECTION).get(SHARD1).get(1));
  }

  private void pullFromTo(JettySolrRunner from, JettySolrRunner to) throws IOException {
    String masterUrl;
    URL url;
    InputStream stream;
    masterUrl = buildUrl(to.getLocalPort())
        + "/replication?wait=true&command=fetchindex&masterUrl="
        + buildUrl(from.getLocalPort()) + "/replication";
    url = new URL(masterUrl);
    stream = url.openStream();
    stream.close();
  }


  /**
   * Invokes a CDCR action on a given node.
   */
  protected NamedList invokePullFromTo(CloudJettyRunner from, CloudJettyRunner to) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("wait", "true");
    params.set("command", "fetchindex");
    params.set("masterUrl", to.url + "/replication");

    SolrRequest request = new QueryRequest(params);
    request.setPath("/replication");

    return from.client.request(request);
  }

}
