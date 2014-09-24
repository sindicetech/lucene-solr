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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.core.SolrCore;

public class CdcrUpdateLog extends UpdateLog {

  protected final Map<CdcrLogReader, CdcrLogPointer> logPointers = new HashMap<>();

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    for (CdcrLogReader reader : logPointers.keySet()) { // remove dangling reader in case of a core change
      reader.close();
    }
    super.init(uhandler, core);
  }

  @Override
  protected void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (logPointers.isEmpty()) { // if no pointers defined, fallback to original behaviour
      super.addOldLog(oldLog, removeOld);
    }
    else {
      if (oldLog == null) return;

      numOldRecords += oldLog.numRecords();

      while (removeOld && logs.size() > 0) {
        TransactionLog log = logs.peekLast();
        int nrec = log.numRecords();

        // remove oldest log if nobody points to it
        // TODO: linear search for each log - should we care ?
        if (!this.hasLogPointer(log)) {
          numOldRecords -= nrec;
          logs.removeLast().decref();  // dereference so it will be deleted when no longer in use
          continue;
        }

        break;
      }

      // don't incref... we are taking ownership from the caller.
      logs.addFirst(oldLog);

      // push the tlog to the opened reader
      for (CdcrLogReader reader : logPointers.keySet()) {
        reader.push(oldLog);
      }
    }
  }

  private boolean hasLogPointer(TransactionLog tlog) {
    for (CdcrLogPointer pointer : logPointers.values()) {
      // if we have a pointer not initialised, then do not remove old log as we have a log reader that has not yet
      // picked it up.
      if (!pointer.isInitialised()) {
        return true;
      }

      if (pointer.tlog == tlog) {
        return true;
      }
    }
    return false;
  }

  public CdcrLogReader newLogReader() {
    return new CdcrLogReader(new ArrayList(logs));
  }

  @Override
  public void close(boolean committed, boolean deleteOnClose) {
    for (CdcrLogReader reader : logPointers.keySet()) {
      reader.close();
    }
    super.close(committed, deleteOnClose);
  }

  private static class CdcrLogPointer {

    TransactionLog tlog = null;
    long pointer = 0;

    private CdcrLogPointer() {}

    private void set(TransactionLog tlog, long pointer) {
      this.tlog = tlog;
      this.pointer = pointer;
    }

    private boolean isInitialised() {
      return tlog == null ? false : true;
    }

    @Override
    public String toString() {
      return "CdcrLogPointer(" + tlog + ", " + pointer + ")";
    }

  }

  class CdcrLogReader {

    private TransactionLog currentTlog;
    private TransactionLog.LogReader tlogReader;

    private final Deque<TransactionLog> tlogs;
    private final CdcrLogPointer pointer;

    public CdcrLogReader(List<TransactionLog> tlogs) {
      this.tlogs = new LinkedList<>();
      this.tlogs.addAll(tlogs);

      // Register the pointer in the parent UpdateLog
      pointer = new CdcrLogPointer();
      logPointers.put(this, pointer);

      // Reader is initialised while the updates log is empty
      if ((currentTlog = this.tlogs.peekLast()) != null) {
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog, tlogReader.currentPos());
      }
    }

    private void push(TransactionLog tlog) {
      this.tlogs.push(tlog);

      // Reader was initialised while the update logs was empty, or reader was exhausted previously
      if (currentTlog == null && !tlogs.isEmpty()) {
        currentTlog = tlogs.peekLast();
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog, tlogReader.currentPos());
      }
    }

    /**
     * Advances to the next log entry in the updates log and returns the log entry itself.
     * Returns null if there are no more log entries in the updates log.<br>
     *
     * <b>NOTE:</b> after the reader has exhausted you can call again this method, as the updates
     * log might have been updated with new entries.
     */
    public Object next() throws IOException, InterruptedException {
      while (!tlogs.isEmpty()) {
        Object o = tlogReader.next();

        if (o != null) {
          pointer.set(currentTlog, tlogReader.currentPos());
          return o;
        }

        tlogReader.close();
        tlogs.removeLast();
        if ((currentTlog = tlogs.peekLast()) != null) {
          tlogReader = currentTlog.getReader(0);
        }
      }

      return null;
    }

    /**
     * Advances to the first beyond the current whose version number is greater
     * than or equal to <i>targetVersion</i>, and returns the log entry itself.
     * Returns null if <i>targetVersion</i> is greater than the highest version number in the updates log.
     */
    public Object seek(long targetVersion) throws IOException, InterruptedException {
      Object o;

      while ((o = this.next()) != null) {
        if (this.getVersion(o) >= targetVersion) {
          return o;
        }
      }

      return null;
    }

    private long getVersion(Object o) {
      // should currently be a List<Oper,Ver,Doc/Id>
      List entry = (List)o;
      return (Long) entry.get(1);
    }

    /**
     * Close streams and remove the associated {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogPointer} from the
     * parent {@link org.apache.solr.update.CdcrUpdateLog}.
     */
    public void close() {
      if (tlogReader != null) {
        tlogReader.close();
        tlogReader = null;
        currentTlog = null;
      }
      tlogs.clear();
      logPointers.remove(this);
    }

  }

}
