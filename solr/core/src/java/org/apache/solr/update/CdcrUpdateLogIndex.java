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
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.solr.common.util.FastInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcrUpdateLogIndex {

  public static Logger log = LoggerFactory.getLogger(CdcrUpdateLogIndex.class);

  public static String ULOG_INDEX_NAME = "ulog-index";
  public static String ULOG_INDEX_SUFFIX = "udx";

  private final File indexFile;

  private final List<Long> logIds = new ArrayList<>();
  private final List<Long> versions = new ArrayList<>();

  CdcrUpdateLogIndex(File tlogDir) {
    indexFile = this.getIndexFile(tlogDir);
  }

  private long getLastLogId() {
    return logIds.isEmpty() ? -1 : logIds.get(logIds.size() - 1);
  }

  void put(long logId, long version) throws IOException {
    if (!this.has(logId)) {
      assert this.getLastLogId() + 1 == logId;
      logIds.add(logId);
      versions.add(version);
      // perform gap encoding and append the new entry to the index file
      long deltaLogId = logIds.size() == 1 ? logId : logId - logIds.get(logIds.size() - 2);
      long deltaVersion = versions.size() == 1 ? version : version - versions.get(versions.size() - 2);
      this.append(logIds.size(), deltaLogId, deltaVersion);
    }
  }

  /**
   * Return the id of the log file that might contain the version number, or -1 if the version is lower than the lower
   * bound of the oldest log file.
   */
  long seek(long version) {
    int i = Collections.binarySearch(versions, version);
    if (i < 0) {
      i = -(i + 1) - 1;
    }
    return i == -1 ? -1 : logIds.get(i);
  }

  boolean has(long logId) {
    return logId <= this.getLastLogId();
  }

  private void append(int size, long deltaLogId, long deltaVersion) throws IOException {
    assert deltaLogId >= 0;
    assert deltaVersion >= 0;

    try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
      // update size
      raf.writeInt(size);

      // seek end of file to append new entry
      raf.seek(raf.length());
      FileChannel channel = raf.getChannel();
      OutputStreamDataOutput os = new OutputStreamDataOutput(Channels.newOutputStream(channel));

      // append new entry
      os.writeVLong(deltaLogId);
      os.writeVLong(deltaVersion);
    }
  }

  void load() throws IOException {
    if (indexFile.exists()) {
      try (InputStreamDataInput is = new InputStreamDataInput(new FastInputStream(new FileInputStream(indexFile)))) {
        int n = is.readInt();

        // we assume there must be at least 1 entry
        logIds.add(is.readVLong());
        versions.add(is.readVLong());

        // perform gap decoding
        for (int i = 1; i < n; i++) {
          logIds.add(logIds.get(i - 1) + is.readVLong());
          versions.add(versions.get(i - 1) + is.readVLong());
        }
      }
    }
  }

  private File getIndexFile(File tlogDir) {
    return new File(tlogDir, ULOG_INDEX_NAME + "." + ULOG_INDEX_SUFFIX);
  }

}
