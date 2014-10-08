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

/**
 * Extends {@link org.apache.solr.update.TransactionLog} to reopen automatically the
 * output stream if its reference count reached 0.
 */
public class CdcrTransactionLog extends TransactionLog {

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings) {
    super(tlogFile, globalStrings);
  }

  CdcrTransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    super(tlogFile, globalStrings, openExisting);
  }

  @Override
  public void incref() {
    // if the refcount is 0, we need to reopen the output stream
    if (refcount.getAndIncrement() == 0) {
      reopenOutputStream(); // synchronised with this
    }
  }

  @Override
  protected void close() {
    try {
      if (debug) {
        log.debug("Closing tlog" + this);
      }

      synchronized (this) {
        if (fos != null) {
          fos.flush();
          fos.close();

          // dereference these variables for GC
          fos = null;
          os = null;
          channel = null;
          raf = null;
        }
      }

      if (deleteOnClose) {
        tlogFile.delete();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Re-open the output stream of the tlog and position
   * the file pointer at the end of the file. It assumes
   * that the tlog is non-empty and that the tlog's header
   * has been already read.
   */
  synchronized void reopenOutputStream() {
    try {
      if (debug) {
        log.debug("Re-opening tlog's output stream: " + this);
      }

      raf = new RandomAccessFile(this.tlogFile, "rw");
      channel = raf.getChannel();
      long start = raf.length();
      raf.seek(start);
      os = Channels.newOutputStream(channel);
      fos = new FastOutputStream(os, new byte[65536], 0);
      fos.setWritten(start);    // reflect that we aren't starting at the beginning
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

}
