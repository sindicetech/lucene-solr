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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.BeforeClass;


public class CdcrVersionPreservationTest extends AbstractCdcrDistributedZkTest {
  private static String vfield = DistributedUpdateProcessor.VERSION_FIELD;//"asdf";//DistributedUpdateProcessor.VERSION_FIELD;

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-tlog-cdcr.xml";
  }

  public CdcrVersionPreservationTest() {
    schemaString = "schema15.xml";      // we need a string id
    //super.sliceCount = 2;
    //super.shardCount = 4;
    //super.fixShardCount = true;  // we only want to test with exactly 2 slices.
  }

  @Override
  public void doTest() throws Exception {
    this.printLayout();
    boolean testFinished = false;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(false);

      //doTestDocVersions();
      doTestCdcrDocVersions();

      commit(); // work arround SOLR-5628

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }
    }
  }

  private void doTestCdcrDocVersions() throws Exception {
    log.info("### STARTING doCdcrTestDocVersions");
    assertEquals(2, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());

    ss = cloudClient;

    vadd("doc1", 10, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc2", 11, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc3", 10, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc4", 11, CdcrUpdateProcessor.CDCR_UPDATE, "");
    commit();

//    doQuery(cloudClient, "doc1,10,doc2,11,doc3,10,doc4,11", "q","*:*");


//    List<String> replicas = getReplicaUrls(DEFAULT_COLLECTION, "shard1");
//    replicas.remove(getLeaderUrl(DEFAULT_COLLECTION, "shard1"));
//
//    for (String replica : replicas) {
//      HttpSolrServer replicaServer = new HttpSolrServer(replica);
//      replicaServer.setConnectionTimeout(15000);
//      doQuery(replicaServer, "doc4,11", "q", "*:*");
//    }

    doQueryShard("shard1",  "doc4,11", "q", "*:*");
    doQueryShard("shard2",  "doc4,11", "q", "*:*");

    //doc1,10,doc2,11,doc3,10,
    //doRTG("doc1,doc2,doc3,doc4", "10,11,10,11");
  }

  private void doTestCdcr() throws Exception {
    log.info("### STARTING doCdcrTestDocVersions");
    assertEquals(2, cloudClient.getZkStateReader().getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());

    ss = cloudClient;

    vadd("doc1", 10, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc2", 11, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc3", 10, CdcrUpdateProcessor.CDCR_UPDATE, "");
    vadd("doc4", 11, CdcrUpdateProcessor.CDCR_UPDATE, "");
    commit();

//    doQuery(cloudClient, "doc1,10,doc2,11,doc3,10,doc4,11", "q","*:*");


//    List<String> replicas = getReplicaUrls(DEFAULT_COLLECTION, "shard1");
//    replicas.remove(getLeaderUrl(DEFAULT_COLLECTION, "shard1"));
//
//    for (String replica : replicas) {
//      HttpSolrServer replicaServer = new HttpSolrServer(replica);
//      replicaServer.setConnectionTimeout(15000);
//      doQuery(replicaServer, "doc4,11", "q", "*:*");
//    }

    doQueryShard("shard1",  "doc4,11", "q", "*:*");
    doQueryShard("shard2",  "doc4,11", "q", "*:*");

    //doc1,10,doc2,11,doc3,10,
    //doRTG("doc1,doc2,doc3,doc4", "10,11,10,11");
  }

  public void doQueryShard(String shard, String expectedDocs, String... queryParams) throws Exception {
    List<String> replicas = getReplicaUrls(DEFAULT_COLLECTION, shard);
    //replicas.remove(getLeaderUrl(DEFAULT_COLLECTION, shard));

    for (String replica : replicas) {
      HttpSolrServer replicaServer = new HttpSolrServer(replica);
      replicaServer.setConnectionTimeout(15000);
      //doQuery(replicaServer, expectedDocs, queryParams);

      QueryResponse rsp = replicaServer.query(params(queryParams));
      log.info("---- RESPONSE "+ replica + " "+shard+": " + rsp.getResults());

      assertEquals(4, rsp.getResults().getNumFound());
    }
  }

  SolrServer ss;

  void vdelete(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setParam("del_version", Long.toString(version));
    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    ss.request(req);
    // req.process(cloudClient);
  }

  void vadd(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.add(sdoc("id", id, vfield, version));
    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    ss.request(req);
  }

  void vaddFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vadd(id, version, params);
    } catch (SolrException e) {
      failed = true;
      assertEquals(errCode, e.code());
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void vdeleteFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vdelete(id, version, params);
    } catch (SolrException e) {
      failed = true;
      assertEquals(errCode, e.code());
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void doQuery(SolrServer ss, String expectedDocs, String... queryParams) throws Exception {

    List<String> strs = StrUtils.splitSmart(expectedDocs, ",", true);
    Map<String, Object> expectedIds = new HashMap<>();
    for (int i=0; i<strs.size(); i+=2) {
      String id = strs.get(i);
      String vS = strs.get(i+1);
      Long v = Long.valueOf(vS);
      expectedIds.put(id,v);
    }

    QueryResponse rsp = ss.query(params(queryParams));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  void doRTG(String ids, String versions) throws Exception {
    Map<String, Object> expectedIds = new HashMap<>();
    List<String> strs = StrUtils.splitSmart(ids, ",", true);
    List<String> verS = StrUtils.splitSmart(versions, ",", true);
    for (int i=0; i<strs.size(); i++) {
      expectedIds.put(strs.get(i), Long.valueOf(verS.get(i)));
    }

    ss.query(params("qt","/get", "ids",ids));

    QueryResponse rsp = cloudClient.query(params("qt","/get", "ids",ids));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  void doRTG(String ids) throws Exception {
    ss.query(params("qt","/get", "ids",ids));

    Set<String> expectedIds = new HashSet<>( StrUtils.splitSmart(ids, ",", true) );

    QueryResponse rsp = cloudClient.query(params("qt","/get", "ids",ids));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  // TODO: refactor some of this stuff into the SolrJ client... it should be easier to use
  void doDBQ(String q, String... reqParams) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(q);
    req.setParams(params(reqParams));
    req.process(cloudClient);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
