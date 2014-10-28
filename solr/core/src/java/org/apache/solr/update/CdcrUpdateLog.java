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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the {@link org.apache.solr.update.UpdateLog} for the CDCR scenario.<br/>
 * Compared to the original update log implementation, transaction logs are removed based on
 * pointers instead of a fixed size limit. Pointers are created by the CDC replicators and
 * correspond to replication checkpoints. If all pointers are ahead of a transaction log,
 * this transaction log is removed.<br/>
 * Given that the number of transaction logs can become considerable if some pointers are
 * lagging behind, the {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader} provides
 * a {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader#seek(long)} method to
 * efficiently lookup a particular transaction log file given a version number.
 */
public class CdcrUpdateLog extends UpdateLog {

  protected final Map<CdcrLogReader, CdcrLogPointer> logPointers = new HashMap<>();

  /**
   * A reader that will be used as toggle to turn on/off the buffering of tlogs
   */
  private CdcrLogReader bufferToggle;

  protected static Logger log = LoggerFactory.getLogger(CdcrUpdateLog.class);

  @Override
  public void init(UpdateHandler uhandler, SolrCore core) {
    // remove dangling readers
    for (CdcrLogReader reader : logPointers.keySet()) {
      reader.close();
    }
    logPointers.clear();

    // init
    super.init(uhandler, core);
  }

  @Override
  protected void addOldLog(TransactionLog oldLog, boolean removeOld) {
    if (oldLog == null) return;

    numOldRecords += oldLog.numRecords();

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
        // remove the oldest log if nobody points to it
        if (!this.hasLogPointer(log)) {
          currRecords -= nrec;
          numOldRecords -= nrec;
          TransactionLog last = logs.removeLast();
          last.deleteOnClose = true;
          last.close();  // it will be deleted if no longer in use
          continue;
        }
        // we have one log with one pointer, we should stop removing logs
        break;
      }

      break;
    }

    // Decref old log as we do not write to it anymore
    // If the oldlog is uncapped, i.e., a write commit has to be performed
    // during recovery, the output stream will be automatically re-open when
    // TransaactionLog#incref will be called.
    oldLog.deleteOnClose = false;
    oldLog.decref();

    // don't incref... we are taking ownership from the caller.
    logs.addFirst(oldLog);
  }

  /**
   * Checks if one of the log pointer is pointing to the given tlog.
   */
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

  /**
   * Creates a new {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}
   * initialised with the current list of tlogs.
   */
  public CdcrLogReader newLogReader() {
    return new CdcrLogReader(new ArrayList(logs));
  }

  /**
   * Enable the buffering of the tlogs. When buffering is activated, the update logs will not remove any
   * old transaction log files.
   */
  public void enableBuffer() {
    if (bufferToggle == null) {
      bufferToggle = this.newLogReader();
    }
  }

  /**
   * Disable the buffering of the tlogs.
   */
  public void disableBuffer() {
    if (bufferToggle != null) {
      bufferToggle.close();
      bufferToggle = null;
    }
  }

  /**
   * Is the update log buffering the tlogs ?
   */
  public boolean isBuffering() {
    return bufferToggle == null ? false : true;
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

  public class CdcrLogReader {

    private TransactionLog currentTlog;
    private TransactionLog.LogReader tlogReader;

    private final Deque<TransactionLog> tlogs;
    private final CdcrLogPointer pointer;

    /**
     * Used to record the last position of the tlog
     */
    private long lastPositionInTLog = 0;

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

    public CdcrLogReader getSubReader() {
      CdcrLogReader clone = new CdcrLogReader((List<TransactionLog>) tlogs);
      clone.lastPositionInTLog = this.lastPositionInTLog;
      if (tlogReader != null) { // if the update log is emtpy, the tlogReader is equal to null
        clone.tlogReader.close();
        clone.tlogReader = currentTlog.getReader(this.tlogReader.currentPos());
      }
      return clone;
    }

    /**
     * Expert: Fast forward this log reader with a log subreader. In order to avoid unexpected results, the log
     * subreader must be created from this reader.
     */
    public void forwardSeek(CdcrLogReader subReader) {
      tlogReader.close(); // close the existing reader, a new one will be created
      while (this.tlogs.peekLast().id < subReader.tlogs.peekLast().id) {
        tlogs.removeLast();
        currentTlog = tlogs.peekLast();
      }
      assert this.tlogs.peekLast().id == subReader.tlogs.peekLast().id;
      this.lastPositionInTLog = subReader.lastPositionInTLog;
      this.tlogReader = currentTlog.getReader(subReader.tlogReader.currentPos());
    }

    /**
     * Advances to the next log entry in the updates log and returns the log entry itself.
     * Returns null if there are no more log entries in the updates log.<br>
     *
     * <b>NOTE:</b> after the reader has exhausted, you can call again this method since the updates
     * log might have been updated with new entries.
     */
    public Object next() throws IOException, InterruptedException {
      while (!tlogs.isEmpty()) {
        lastPositionInTLog = tlogReader.currentPos();
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
          this.resetToLastPosition();
          return true;
        }
      }

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
     * If called after {@link #next()}, it resets the reader to its last position.
     */
    public void resetToLastPosition() {
      try {
        if (tlogReader != null) {
          tlogReader.fis.seek(lastPositionInTLog);
        }
      }
      catch (IOException e) {
        log.error("Failed to seek last position in tlog", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to seek last position in tlog", e);
      }
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
