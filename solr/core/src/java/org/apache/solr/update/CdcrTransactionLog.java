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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.Collection;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.FastOutputStream;

public class CdcrTransactionLog extends TransactionLog {

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings) {
    super(tlogFile, globalStrings);
  }

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    super(tlogFile, globalStrings, openExisting);
  }

  @Override
  public void incref() {
    if (refcount.getAndIncrement() == 0) {
      openOutputStream(); // synchronised with this
    }
  }

  @Override
  public LogReader getReader(long startingPos) {
    // don't incref... we are taking ownership from the caller.
    return new LogReader(true, startingPos);
  }

  @Override
  public ReverseReader getReverseReader() throws IOException {
    // don't incref... we are taking ownership from the caller.
    return new FSReverseReader(true);
  }

  synchronized void openOutputStream() {
    try {
      if (debug) {
        log.debug("Opening tlog's output stream: " + this);
      }

      raf = new RandomAccessFile(this.tlogFile, "rw");
      channel = raf.getChannel();
      os = Channels.newOutputStream(channel);
      fos = new FastOutputStream(os, new byte[65536], 0);
      fos.setWritten(raf.length());    // reflect that we aren't starting at the beginning
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}
