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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor;


public class CdcrVersionReplicationTest extends AbstractCdcrDistributedZkTest {
  private static final String vfield = DistributedUpdateProcessor.VERSION_FIELD;
  SolrServer solrServer;

  /*
  TODO: verify that:
    shard1: doc1, doc2, doc3
    shard2: doc4
   */

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-tlog-cdcr.xml";
  }

  public CdcrVersionReplicationTest() {
    schemaString = "schema15.xml";      // we need a string id
//    super.sliceCount = 1;
//    super.shardCount = 2;
//    super.fixShardCount = true;  // we only want to test with exactly 2 slices.
  }

  SolrServer createClientRandomly() throws Exception {
    int r = random().nextInt(100);

    // testing the smart cloud client (requests to leaders) is more important than testing the forwarding logic
    if (r < 80) {
      return createCloudClient(DEFAULT_COLLECTION);
    }

    if (r < 90) {
      return getReplicaClient(DEFAULT_COLLECTION, "shard1");
    }

    return getReplicaClient(DEFAULT_COLLECTION, "shard2");
  }

  @Override
  public void doTest() throws Exception {
    this.printLayout();
    boolean testFinished = false;
    SolrServer client = null;
    try {
      handle.clear();
      handle.put("timestamp", SKIPVAL);

      // todo: do I have to do this here?
      waitForRecoveriesToFinish(DEFAULT_COLLECTION, false);

      client = createClientRandomly();
      doTestCdcrDocVersions(client);

      commit(); // work arround SOLR-5628

      testFinished = true;
    } finally {
      if (!testFinished) {
        printLayoutOnTearDown = true;
      }

      if (client != null) {
        client.shutdown();
      }
    }
  }

  private void doTestCdcrDocVersions(SolrServer solrServer) throws Exception {
    this.solrServer = solrServer;

    log.info("### STARTING doCdcrTestDocVersions - Add commands, client: " + solrServer);

    vadd("doc1", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc2", 11, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "11");
    vadd("doc3", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc4", 11, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "11");
    commit();

    // versions are preserved and verifiable both by query and by real-time get
    doQuery(solrServer, "doc1,10,doc2,11,doc3,10,doc4,11", "q","*:*");
    doRealTimeGet("doc1,doc2,doc3,doc4", "10,11,10,11");

    vadd("doc1", 5, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "5");
    vadd("doc2", 10, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "10");
    vadd("doc3", 9, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "9");
    vadd("doc4", 8, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "8");

    // lower versions are ignored
    doRealTimeGet("doc1,doc2,doc3,doc4", "10,11,10,11");

    vadd("doc1", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc2", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc3", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");
    vadd("doc4", 12, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "12");

    // higher versions are accepted
    doRealTimeGet("doc1,doc2,doc3,doc4", "12,12,12,12");

    // non-cdcr update requests throw a version conflict exception for non-equal versions (optimistic locking feature)
    vaddFail("doc1", 13, 409);
    vaddFail("doc2", 13, 409);
    vaddFail("doc3", 13, 409);

    commit();

    // versions are still as they were
    doQuery(solrServer, "doc1,12,doc2,12,doc3,12,doc4,12", "q","*:*");

    // query all shard replicas individually
    doQueryShardReplica("shard1", "doc1,12,doc2,12,doc3,12,doc4,12", "q", "*:*");
    doQueryShardReplica("shard2", "doc1,12,doc2,12,doc3,12,doc4,12", "q","*:*");

    // optimistic locking update
    vadd("doc4", 12);
    commit();

    QueryResponse rsp = solrServer.query(params("qt","/get", "ids", "doc4"));
    long version = (long) rsp.getResults().get(0).get(vfield);

    // update accepted and a new version number was generated
    assertTrue(version > 1_000_000_000_000l);

    log.info("### STARTING doCdcrTestDocVersions - Delete commands");

    // send a delete update with an older version number
    vdelete("doc1", 5, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "5");
    // must ignore the delete
    doRealTimeGet("doc1", "12");

    // send a delete update with a higher version number
    vdelete("doc1", 13, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, "13");
    // must be deleted
    doRealTimeGet("doc1", "");

    // send a delete update with a higher version number
    vdelete("doc4", version + 1, CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, ""+(version+1));
    // must be deleted
    doRealTimeGet("doc4", "");

    commit();

    // query each shard replica individually
    doQueryShardReplica("shard1", "doc2,12,doc3,12", "q", "*:*");
    doQueryShardReplica("shard2", "doc2,12,doc3,12", "q", "*:*");

    // version conflict thanks to optimistic locking
    if (solrServer instanceof CloudSolrServer) // TODO: it seems that optimistic locking doesn't work with forwarding, test with shard2 client
      vdeleteFail("doc2", 50, 409);

    // cleanup after ourselves for the next run
    // deleteByQuery should work as usual with the CDCR_UPDATE param
    doDeleteByQuery("id:doc*", CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, Long.toString(1));
    commit();

    // deleteByQuery with a version lower than anything else should have no effect
    doQuery(solrServer, "doc2,12,doc3,12", "q", "*:*");

    doDeleteByQuery("id:doc*", CdcrUpdateProcessor.CDCR_UPDATE, "", vfield, Long.toString(51));
    commit();

    // deleteByQuery with a version higher than everything else should delete all remaining docs
    doQuery(solrServer, "", "q", "*:*");

    // check that replicas are as expected too
    doQueryShardReplica("shard1", "", "q", "*:*");
    doQueryShardReplica("shard2", "", "q", "*:*");
  }


  // ------------------ auxiliary methods ------------------


  void doQueryShardReplica(String shard, String expectedDocs, String... queryParams) throws Exception {
    List<String> replicas = getReplicaUrls(DEFAULT_COLLECTION, shard);

    for (String replica : replicas) {
      HttpSolrServer replicaServer = new HttpSolrServer(replica);
      replicaServer.setConnectionTimeout(15000);
      doQuery(replicaServer, expectedDocs, queryParams);

      QueryResponse rsp = replicaServer.query(params(queryParams));
      log.info("---- RESPONSE "+ replica + " "+shard+": " + rsp.getResults());
    }
  }

  void vdelete(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setParam(vfield, Long.toString(version));

    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    solrServer.request(req);
  }

  void vdeleteFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vdelete(id, version, params);
    } catch (SolrException e) {
      failed = true;
      if (e.getCause() instanceof SolrException && e.getCause() != e) {
        e = (SolrException) e.getCause();
      }
      assertEquals(errCode, e.code());
    } catch (SolrServerException ex) {
      Throwable t = ex.getCause();
      if (t instanceof SolrException) {
        failed = true;
        SolrException exception = (SolrException) t;
        assertEquals(errCode, exception.code());
      }
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  void vadd(String id, long version, String... params) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.add(sdoc("id", id, vfield, version));
    for (int i=0; i<params.length; i+=2) {
      req.setParam( params[i], params[i+1]);
    }
    solrServer.request(req);
  }

  void vaddFail(String id, long version, int errCode, String... params) throws Exception {
    boolean failed = false;
    try {
      vadd(id, version, params);
    } catch (SolrException e) {
      failed = true;
      if (e.getCause() instanceof SolrException && e.getCause() != e) {
        e = (SolrException) e.getCause();
      }
      assertEquals(errCode, e.code());
    } catch (SolrServerException ex) {
      Throwable t = ex.getCause();
      if (t instanceof SolrException) {
        failed = true;
        SolrException exception = (SolrException) t;
        assertEquals(errCode, exception.code());
      }
    } catch (Exception e) {
      log.error("ERROR", e);
    }
    assertTrue(failed);
  }

  String vaddFailMessage(String id, long version, String... params) {
    String msg = null;
    try {
      vadd(id, version, params);
    } catch (Exception ex) {
      msg = ex.getMessage();
    }
    return msg;
  }

  void doQueryReplica(String collection, String shardId, String expectedDocs, String... queryParams) throws Exception {
    doQuery(getReplicaClient(collection, shardId), expectedDocs, queryParams);
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


  void doRealTimeGet(String ids, String versions) throws Exception {
    Map<String, Object> expectedIds = new HashMap<>();
    List<String> strs = StrUtils.splitSmart(ids, ",", true);
    List<String> verS = StrUtils.splitSmart(versions, ",", true);
    for (int i=0; i<strs.size(); i++) {
      if (! verS.isEmpty()) {
        expectedIds.put(strs.get(i), Long.valueOf(verS.get(i)));
      }
    }

    QueryResponse rsp = solrServer.query(params("qt","/get", "ids",ids));
    Map<String, Object> obtainedIds = new HashMap<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.put((String) doc.get("id"), doc.get(vfield));
    }

    assertEquals(expectedIds, obtainedIds);
  }

  void doRealTimeGet(SolrServer ss, String ids) throws Exception {
    Set<String> expectedIds = new HashSet<>( StrUtils.splitSmart(ids, ",", true) );

    QueryResponse rsp = solrServer.query(params("qt","/get", "ids",ids));
    Set<String> obtainedIds = new HashSet<>();
    for (SolrDocument doc : rsp.getResults()) {
      obtainedIds.add((String) doc.get("id"));
    }

    assertEquals(expectedIds, obtainedIds);
  }


  void doDeleteByQuery(String q, String... reqParams) throws Exception {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(q);
    req.setParams(params(reqParams));
    req.process(solrServer);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
