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
package org.apache.solr.update;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class TestCdcrUpdateLog extends SolrTestCaseJ4 {

  // means that we've seen the leader and have version info (i.e. we are a non-leader replica)
  private static String FROM_LEADER = DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString();

  private static int timeout=60;  // acquire timeout in seconds.  change this to a huge number when debugging to prevent threads from advancing.

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    initCore("solrconfig-tlog.xml","schema15.xml");
  }

  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  // since we make up fake versions in these tests, we can get messed up by a DBQ with a real version
  // since Solr can think following updates were reordered.
  @Override
  public void clearIndex() {
    try {
      deleteByQueryAndGetVersion("*:*", params("_version_", Long.toString(-Long.MAX_VALUE), DISTRIB_UPDATE_PARAM,FROM_LEADER));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void clearCore() {
    clearIndex();
    assertU(commit());

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    h.close();

    String[] files = ulog.getLogList(logDir);
    for (String file : files) {
      new File(logDir, file).delete();
    }

    assertEquals(0, ulog.getLogList(logDir).length);

    createCore();
  }

  private void addDocs(int nDocs, int start, LinkedList<Long> versions) throws Exception {
    for (int i = 0; i < nDocs; i++) {
      versions.addFirst(addAndGetVersion(sdoc("id",Integer.toString(start + i)), null));
    }
  }

  private static Long getVer(SolrQueryRequest req) throws Exception {
    String response = JQ(req);
    Map rsp = (Map) ObjectBuilder.fromJSON(response);
    Map doc = null;
    if (rsp.containsKey("doc")) {
      doc = (Map)rsp.get("doc");
    } else if (rsp.containsKey("docs")) {
      List lst = (List)rsp.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    } else if (rsp.containsKey("response")) {
      Map responseMap = (Map)rsp.get("response");
      List lst = (List)responseMap.get("docs");
      if (lst.size() > 0) {
        doc = (Map)lst.get(0);
      }
    }

    if (doc == null) return null;

    return (Long)doc.get("_version_");
  }

  @Test
  public void testLogReaderNext() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader(); // test reader on empty updates log

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions); start+=10;
    assertU(commit());

    addDocs(11, start, versions);  start+=11;
    assertU(commit());

    for (int i = 0; i < 10; i++) { // 10 adds
      assertNotNull(reader.next());
    }
    Object o = reader.next();
    assertNotNull(o);

    List entry = (List) o;
    int opAndFlags = (Integer)entry.get(0);
    assertEquals(UpdateLog.COMMIT, opAndFlags & UpdateLog.OPERATION_MASK);

    for (int i = 0; i < 11; i++) { // 11 adds
      assertNotNull(reader.next());
    }
    o = reader.next();
    assertNotNull(o);

    entry = (List) o;
    opAndFlags = (Integer)entry.get(0);
    assertEquals(UpdateLog.COMMIT, opAndFlags & UpdateLog.OPERATION_MASK);

    assertNull(reader.next());

    // add a new tlog after having exhausted the reader

    addDocs(10, start, versions); start+=10;
    assertU(commit());

    // the reader should pick up the new tlog

    for (int i = 0; i < 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader.next());
    }
    assertNull(reader.next());
  }

  @Test
  public void testLogReaderSeek() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader1 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader2 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader3 = ((CdcrUpdateLog) ulog).newLogReader();

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions); start+=10;
    assertU(commit());

    addDocs(11, start, versions); start+=11;
    assertU(commit());

    addDocs(10, start, versions); start+=10;
    assertU(commit());

    // Test case where target version is equal to startVersion of tlog file
    long targetVersion = getVer(req("q","id:10"));

    assertTrue(reader1.seek(targetVersion));
    Object o = reader1.next();
    assertNotNull(o);
    List entry = (List) o;
    long version = (Long) entry.get(1);

    assertEquals(targetVersion, version);

    assertNotNull(reader1.next());

    // test case where target version is superior to startVersion of tlog file
    targetVersion = getVer(req("q","id:26"));

    assertTrue(reader2.seek(targetVersion));
    o = reader2.next();
    assertNotNull(o);
    entry = (List) o;
    version = (Long) entry.get(1);

    assertEquals(targetVersion, version);

    assertNotNull(reader2.next());

    // test case where target version is inferior to startVersion of oldest tlog file
    targetVersion = getVer(req("q","id:0")) - 1;

    assertFalse(reader3.seek(targetVersion));
  }

  @Test
  public void testLogReaderNextOnNewTLog() throws Exception {
    this.clearCore();

    int start = 0;

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader();

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions); start+=10;
    assertU(commit());

    addDocs(11, start, versions);  start+=11;

    for (int i = 0; i < 22; i++) { // 21 adds + 1 commit
      assertNotNull(reader.next());
    }

    // we should have reach the end of the new tlog
    assertNull(reader.next());

    addDocs(5, start, versions);  start+=5;

    // the reader should now pick up the new updates

    for (int i = 0; i < 5; i++) { // 5 adds
      assertNotNull(reader.next());
    }

    assertNull(reader.next());

  }

  @Test
  public void testRemoveOldLogs() throws Exception {
    this.clearCore();

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    int start = 0;
    int maxReq = 50;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions); start+=10;
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
    assertU(commit());
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

    addDocs(10, start, versions);  start+=10;
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
    assertU(commit());
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

    assertEquals(2, ulog.getLogList(logDir).length);

    // Get a cdcr log reader to initialise a log pointer
    CdcrUpdateLog.CdcrLogReader reader = ((CdcrUpdateLog) ulog).newLogReader();

    addDocs(105, start, versions);  start+=105;
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
    assertU(commit());
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

    // the previous two tlogs should not be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    // move the pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader.next());
    }

    addDocs(10, start, versions);  start+=10;
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
    assertU(commit());
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

    // the first tlog should be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    h.close();
    createCore();

    addDocs(105, start, versions);  start+=105;
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));
    assertU(commit());
    assertJQ(req("qt","/get", "getVersions",""+maxReq), "/versions==" + versions.subList(0,Math.min(maxReq,start)));

    // previous tlogs should be gone now
    assertEquals(1, ulog.getLogList(logDir).length);
  }

  @Test
  public void testRemoveOldLogsMultiplePointers() throws Exception {
    this.clearCore();

    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    CdcrUpdateLog.CdcrLogReader reader1 = ((CdcrUpdateLog) ulog).newLogReader();
    CdcrUpdateLog.CdcrLogReader reader2 = ((CdcrUpdateLog) ulog).newLogReader();

    int start = 0;

    LinkedList<Long> versions = new LinkedList<>();
    addDocs(10, start, versions); start+=10;
    assertU(commit());

    addDocs(10, start, versions);  start+=10;
    assertU(commit());

    addDocs(105, start, versions);  start+=105;
    assertU(commit());

    // the previous two tlogs should not be removed
    assertEquals(3, ulog.getLogList(logDir).length);

    // move the first pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader1.next());
    }

    addDocs(10, start, versions);  start+=10;
    assertU(commit());

    // the first tlog should not be removed
    assertEquals(4, ulog.getLogList(logDir).length);

    // move the second pointer past the first tlog
    for (int i = 0; i <= 11; i++) { // 10 adds + 1 commit
      assertNotNull(reader2.next());
    }

    addDocs(10, start, versions);  start+=10;
    assertU(commit());

    // the first tlog should be removed
    assertEquals(4, ulog.getLogList(logDir).length);
  }

}
