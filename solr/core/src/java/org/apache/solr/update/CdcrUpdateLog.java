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
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;

public class CdcrUpdateLog extends UpdateLog {

  protected final Map<CdcrLogReader, CdcrLogPointer> logPointers = new HashMap<>();

  private CdcrUpdateLogIndex index;

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    // remove dangling reader in case of a core change
    for (CdcrLogReader reader : logPointers.keySet()) {
      reader.close();
    }

    // init
    super.init(uhandler, core);

    // load index
    index = new CdcrUpdateLogIndex(new File(this.getLogDir()));
    try {
      index.load();
    }
    catch (IOException e) {
      // failure to load the index isn't fatal, it will be generated
      log.error("Unable to load the updateLog index: " + e.getMessage(), e);
    }
  }

  @Override
  protected void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (oldLog == null) return;

    if (logPointers.isEmpty()) { // if no pointers defined, fallback to original behaviour
      super.addOldLog(oldLog, removeOld);
    }
    else {
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
    }

    // add entry to the index
    if (!index.has(oldLog.id)) {
      try {
        TransactionLog.LogReader reader = oldLog.getReader(0);
        long version = (Long) ((List) reader.next()).get(1);
        reader.close();
        index.put(oldLog.id, version);
      } catch (Exception e) {
        log.error("Unable to update the updateLog index: " + e.getMessage(), e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unable to update the updateLog index: " + e.getMessage(), e);
      }
    }
  }

  private boolean hasLogPointer(TransactionLog tlog) {
    for (CdcrLogPointer pointer : logPointers.values()) {
      // if we have a pointer not initialised, then do not remove old tlogs as we have a log reader that has not yet
      // picked them up.
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
  protected void ensureLog() {
    if (tlog == null) {
      super.ensureLog();

      // push the new tlog to the opened readers
      for (CdcrLogReader reader : logPointers.keySet()) {
        reader.push(tlog);
      }
    }
  }

  @Override
  public void close(boolean committed, boolean deleteOnClose) {
    for (CdcrLogReader reader : new ArrayList<>(logPointers.keySet())) {
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

    private CdcrLogReader(List<TransactionLog> tlogs) {
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

        if (tlogs.size() > 1) { // the current tlog is not the newest one, we can advance to the next one
          tlogReader.close();
          tlogs.removeLast();
          currentTlog = tlogs.peekLast();
          tlogReader = currentTlog.getReader(0);
        }
        else {
          // the only tlog left is the new tlog which is currently being written,
          // we should not remove it as we have to try to read it again later.
          return null;
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

      // Seek corresponding tlog based on index
      // Iterates over the queue, and removes the last tlog if it does not match
      // If the index returns an unknown logID (probably removed), we will end up with an empty list which is fine. We
      // don't want to initialise a reader if there is a gap.
      long logId = index.seek(targetVersion);
      while (tlogs.size() > 1) { // do not remove the first entry (new tlog)
        if (tlogs.peekLast().id != logId) {
          tlogReader.close();
          tlogs.removeLast();
        }
      }
      currentTlog = tlogs.peekLast();
      tlogReader = currentTlog.getReader(0);

      // now that we might be on the right tlog, iterates over the entries to find the one we are looking for
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
