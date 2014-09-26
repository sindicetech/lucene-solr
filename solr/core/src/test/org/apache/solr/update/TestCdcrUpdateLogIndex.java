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
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.lucene.store.InputStreamDataInput;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCdcrUpdateLogIndex extends SolrTestCaseJ4 {

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

  @Test
  public void testPut() throws IOException {
    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());

    CdcrUpdateLogIndex index = new CdcrUpdateLogIndex(logDir);
    index.put(0, 123);
    index.put(1, 124);

    File indexFile = new File(logDir, CdcrUpdateLogIndex.ULOG_INDEX_NAME + "." + CdcrUpdateLogIndex.ULOG_INDEX_SUFFIX);
    assertTrue(indexFile.exists());
    assertTrue(indexFile.length() > 0);

    try (InputStreamDataInput is = new InputStreamDataInput(new FileInputStream(indexFile))) {
      assertEquals(2, is.readInt());
    }

    // it should not append an entry that already exist in the index
    index.put(1, 124);
    index.put(0, 123);

    try (InputStreamDataInput is = new InputStreamDataInput(new FileInputStream(indexFile))) {
      assertEquals(2, is.readInt());
    }
  }

  @Test
  public void testSeek() throws IOException {
    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    File indexFile = new File(logDir, CdcrUpdateLogIndex.ULOG_INDEX_NAME + "." + CdcrUpdateLogIndex.ULOG_INDEX_SUFFIX);
    if (indexFile.exists()) indexFile.delete();

    CdcrUpdateLogIndex index = new CdcrUpdateLogIndex(logDir);
    index.put(0, 123);
    index.put(1, 124);
    index.put(2, 126);
    index.put(3, 128);

    assertEquals(1, index.seek(125));
    assertEquals(3, index.seek(128));
    assertEquals(3, index.seek(1234));

    assertEquals(-1, index.seek(42));
  }

  @Test
  public void testLoad() throws IOException {
    UpdateLog ulog = h.getCore().getUpdateHandler().getUpdateLog();
    File logDir = new File(h.getCore().getUpdateHandler().getUpdateLog().getLogDir());
    File indexFile = new File(logDir, CdcrUpdateLogIndex.ULOG_INDEX_NAME + "." + CdcrUpdateLogIndex.ULOG_INDEX_SUFFIX);
    if (indexFile.exists()) indexFile.delete();

    CdcrUpdateLogIndex index = new CdcrUpdateLogIndex(logDir);
    index.put(0, 123);
    index.put(1, 124);
    index.put(2, 126);
    index.put(3, 128);

    index = new CdcrUpdateLogIndex(logDir);

    assertEquals(2, index.seek(127));
    assertEquals(0, index.seek(123));
  }

}
