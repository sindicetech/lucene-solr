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

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    // remove dangling reader in case of a core change
    for (CdcrLogReader reader : logPointers.keySet()) {
      reader.close();
    }

    // init
    super.init(uhandler, core);
  }

  @Override
  protected void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (oldLog == null) return;

    numOldRecords += oldLog.numRecords();

    if (logPointers.isEmpty()) { // if no pointers defined, fallback to original behaviour
      int currRecords = numOldRecords;

      if (oldLog != tlog &&  tlog != null) {
        currRecords += tlog.numRecords();
      }

      while (removeOld && logs.size() > 0) {
        TransactionLog log = logs.peekLast();
        int nrec = log.numRecords();
        // remove oldest log if we don't need it to keep at least numRecordsToKeep, or if
        // we already have the limit of 10 log files.
        if (currRecords - nrec >= numRecordsToKeep || logs.size() >= 10) {
          currRecords -= nrec;
          numOldRecords -= nrec;
          TransactionLog last = logs.removeLast();
          last.deleteOnClose = true;
          last.close();  // it will be deleted if no longer in use
          continue;
        }

        break;
      }
    }
    else {
      while (removeOld && logs.size() > 0) {
        TransactionLog log = logs.peekLast();
        int nrec = log.numRecords();

        // remove the oldest log if nobody points to it
        if (!this.hasLogPointer(log)) {
          numOldRecords -= nrec;
          TransactionLog last = logs.removeLast();
          last.deleteOnClose = true;
          last.close();  // it will be deleted if no longer in use
          continue;
        }

        break;
      }
    }

    // decref old log as we do not write to it anymore
    try {
      if (oldLog.endsWithCommit()) {
        oldLog.deleteOnClose = false;
        oldLog.decref();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    // don't incref... we are taking ownership from the caller.
    logs.addFirst(oldLog);
  }

  private boolean hasLogPointer(TransactionLog tlog) {
    for (CdcrLogPointer pointer : logPointers.values()) {
      // if we have a pointer that is not initialised, then do not remove the old tlogs
      // as we have a log reader that didn't pick them up yet.
      if (!pointer.isInitialised()) {
        return true;
      }

      if (pointer.tlogFile == tlog.tlogFile) {
        return true;
      }
    }
    return false;
  }

  public CdcrLogReader newLogReader() {
    return new CdcrLogReader(new ArrayList(logs));
  }

  @Override
  protected void ensureLog(long startVersion) {
    if (tlog == null) {
      super.ensureLog(startVersion);

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

    File tlogFile = null;

    private CdcrLogPointer() {}

    private void set(File tlogFile) {
      this.tlogFile = tlogFile;
    }

    private boolean isInitialised() {
      return tlogFile == null ? false : true;
    }

    @Override
    public String toString() {
      return "CdcrLogPointer(" + tlogFile + ")";
    }

  }

  class CdcrLogReader {

    private TransactionLog currentTlog;
    private TransactionLog.LogReader tlogReader;

    private final Deque<TransactionLog> tlogs;
    private final CdcrLogPointer pointer;

    /**
     * Used by {@link #seek(long)} to keep track of the latest log entry read.
     */
    private Object lookahead = null;

    private CdcrLogReader(List<TransactionLog> tlogs) {
      this.tlogs = new LinkedList<>();
      this.tlogs.addAll(tlogs);

      // Register the pointer in the parent UpdateLog
      pointer = new CdcrLogPointer();
      logPointers.put(this, pointer);

      // If the reader is initialised while the updates log is empty, do nothing
      if ((currentTlog = this.tlogs.peekLast()) != null) {
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog.tlogFile);
      }
    }

    private void push(TransactionLog tlog) {
      this.tlogs.push(tlog);

      // The reader was initialised while the update logs was empty, or reader was exhausted previously,
      // we have to update the current tlog and the associated tlog reader.
      if (currentTlog == null && !tlogs.isEmpty()) {
        currentTlog = tlogs.peekLast();
        tlogReader = currentTlog.getReader(0);
        pointer.set(currentTlog.tlogFile);
      }
    }

    /**
     * Advances to the next log entry in the updates log and returns the log entry itself.
     * Returns null if there are no more log entries in the updates log.<br>
     *
     * <b>NOTE:</b> after the reader has exhausted, you can call again this method since the updates
     * log might have been updated with new entries.
     */
    public Object next() throws IOException, InterruptedException {
      if (lookahead != null) {
        Object o = lookahead;
        lookahead = null;
        return o;
      }

      while (!tlogs.isEmpty()) {
        Object o = tlogReader.next();

        if (o != null) {
          pointer.set(currentTlog.tlogFile);
          return o;
        }

        if (tlogs.size() > 1) { // if the current tlog is not the newest one, we can advance to the next one
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
     * than or equal to <i>targetVersion</i>.<br>
     * Returns true if the reader has been advanced. If <i>targetVersion</i> is
     * greater than the highest version number in the updates log, the reader
     * has been advanced to the end of the current tlog, and a call to
     * {@link #next()} will probably return null.<br>
     * Returns false if <i>targetVersion</i> is lower than the oldest known entry.
     * In this scenario, it probably means that there is a gap in the updates log.<br>
     *
     * <b>NOTE:</b> This method must be called before the first call to {@link #next()}.
     */
    public boolean seek(long targetVersion) throws IOException, InterruptedException {
      Object o;

      if (!this.seekTLog(targetVersion)) {
        return false;
      }

      // now that we might be on the right tlog, iterates over the entries to find the one we are looking for
      while ((o = this.next()) != null) {
        if (this.getVersion(o) >= targetVersion) {
          lookahead = o;
          return true;
        }
      }

      lookahead = null;
      return true;
    }

    /**
     * Seeks the tlog associated to the target version by using the updates log index,
     * and initialises the log reader to the start of the tlog. Returns true if it was able
     * to seek the corresponding tlog, false if the <i>targetVersion</i> is lower than the
     * oldest known entry (which probably indicates a gap).<br>
     *
     * <b>NOTE:</b> This method might modify the tlog queue by removing tlogs that are older
     * than the target version.
     */
    private boolean seekTLog(long targetVersion) {
      // if the target version is lower than the oldest known entry, we have probably a gap.
      if (targetVersion < tlogs.peekLast().startVersion) {
        return false;
      }

      // closes existing reader before performing seek and possibly modifying the queue;
      tlogReader.close();

      // iterates over the queue and removes old tlogs
      TransactionLog last = null;
      while (tlogs.size() > 1) {
        if (tlogs.peekLast().startVersion >= targetVersion) {
          break;
        }
        last = tlogs.pollLast();
      }

      // the last tlog removed is the one we look for, add it back to the queue
      if (last != null) tlogs.addLast(last);

      currentTlog = tlogs.peekLast();
      tlogReader = currentTlog.getReader(0);

      return true;
    }

    private long getVersion(Object o) {
      List entry = (List)o;
      return (Long) entry.get(1);
    }

    /**
     * Closes streams and remove the associated {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogPointer} from the
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
