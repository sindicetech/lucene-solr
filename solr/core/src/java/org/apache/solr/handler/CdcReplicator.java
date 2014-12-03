package org.apache.solr.handler;

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

import java.nio.charset.Charset;
import java.util.List;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.update.processor.CdcrUpdateProcessor;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The replication logic. Given a {@link org.apache.solr.handler.CdcReplicatorState}, it reads all the new entries
 * in the update log and forward them to the target cluster. If an error occurs, the replication is stopped and
 * will be tried again later.
 */
public class CdcReplicator implements Runnable {

  private final CdcReplicatorState state;

  // TODO: make this configurable
  private static final int BATCH_SIZE = Integer.MAX_VALUE;

  protected static Logger log = LoggerFactory.getLogger(CdcReplicator.class);

  public CdcReplicator(CdcReplicatorState state) {
    this.state = state;
  }

  @Override
  public void run() {
    CdcrUpdateLog.CdcrLogReader logReader = state.getLogReader();
    if (logReader == null) {
      log.warn("Log reader for target {} is not initialised, it will be ignored.", state.getTargetCollection());
      return;
    }

    try {
      // create update request
      UpdateRequest req = new UpdateRequest();
      // Add the param to indicate the {@link CdcrUpdateProcessor} to keep the provided version number
      req.setParam(CdcrUpdateProcessor.CDCR_UPDATE, "");

      // Start the benchmakr timer
      state.getBenchmarkTimer().start();

      long counter = 0;
      CdcrUpdateLog.CdcrLogReader subReader = logReader.getSubReader();
      Object o = subReader.next();

      for (int i = 0; i < BATCH_SIZE && o != null; i++, o = subReader.next()) {
        if(isDelete(o)) {

          /*
          * Deletes are sent one at a time.
          */

          // First send out current batch of SolrInputDocument, the non-deletes.
          List<SolrInputDocument> docs = req.getDocuments();

          if(docs != null && docs.size() > 0) {

            subReader.resetToLastPosition(); // Push back the delete for now.
            UpdateResponse rsp = req.process(state.getClient());
            if (rsp.getStatus() != 0) {
              throw new CdcReplicatorException(req, rsp);
            }

            state.resetConsecutiveErrors();
            logReader.forwardSeek(subReader); //Advance the main reader to just befor the delete.
            o = subReader.next(); //Read the delete again
            counter+= docs.size();
            req.clear();
          }

          //Process Delete
          this.processUpdate(o, req);
          UpdateResponse rsp = req.process(state.getClient());
          if (rsp.getStatus() != 0) {
            throw new CdcReplicatorException(req, rsp);
          }

          state.resetConsecutiveErrors();
          logReader.forwardSeek(subReader);
          counter++;
          req.clear();

        } else {

          if (this.processUpdate(o, req) != null) {
            int buffered = req.getDocuments().size();
            if(buffered > 100) {
              UpdateResponse rsp = req.process(state.getClient());
              if (rsp.getStatus() != 0) {
                throw new CdcReplicatorException(req, rsp);
              }
              // we successfully forwarded the update, reset the number of consecutive errors
              state.resetConsecutiveErrors();
              counter+=buffered;
              logReader.forwardSeek(subReader);
              req.clear();
            }
          }
        }
      }

      //Send the final batch out.
      List<SolrInputDocument> docs = req.getDocuments();

      if((docs != null && docs.size() > 0)) {
        UpdateResponse rsp = req.process(state.getClient());
        if (rsp.getStatus() != 0) {
          throw new CdcReplicatorException(req, rsp);
        }

        state.resetConsecutiveErrors();
        logReader.forwardSeek(subReader);
        counter+= docs.size();
      }

      log.info("Forwarded {} updates to target {}", counter, state.getTargetCollection());
    }
    catch (Exception e) {
      // report error and update error stats
      this.handleException(e);
    }
    finally {
      // stop the benchmark timer
      state.getBenchmarkTimer().stop();
    }
  }

  private boolean isDelete(Object o) {
    List entry = (List) o;
    int operationAndFlags = (Integer) entry.get(0);
    int oper = operationAndFlags & UpdateLog.OPERATION_MASK;
    return oper == UpdateLog.DELETE_BY_QUERY || oper == UpdateLog.DELETE;
  }

  private void handleException(Exception e) {
    if (e instanceof CdcReplicatorException) {
      UpdateRequest req = ((CdcReplicatorException) e).req;
      UpdateResponse rsp = ((CdcReplicatorException) e).rsp;
      log.warn("Failed to forward update request {}. Got response {}", req, rsp);
      state.reportError(CdcReplicatorState.ErrorType.BAD_REQUEST);
    }
    else if (e instanceof CloudSolrServer.RouteException) {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcReplicatorState.ErrorType.BAD_REQUEST);
    }
    else {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcReplicatorState.ErrorType.INTERNAL);
    }
  }

  private UpdateRequest processUpdate(Object o, UpdateRequest req) {

    // should currently be a List<Oper,Ver,Doc/Id>
    List entry = (List) o;

    int operationAndFlags = (Integer) entry.get(0);
    int oper = operationAndFlags & UpdateLog.OPERATION_MASK;
    long version = (Long) entry.get(1);

    // record the operation in the benchmark timer
    state.getBenchmarkTimer().incrementCounter(oper);

    switch (oper) {
      case UpdateLog.ADD: {
        SolrInputDocument sdoc = (SolrInputDocument) entry.get(entry.size() - 1);
        req.add(sdoc);
        //SolrInputField f = new SolrInputField(VersionInfo.VERSION_FIELD);
        //f.setValue(version, 1.0f);
        //sdoc.addField(VersionInfo.VERSION_FIELD,f);
        return req;
      }
      case UpdateLog.DELETE: {
        byte[] idBytes = (byte[]) entry.get(2);
        req.deleteById(new String(idBytes, Charset.forName("UTF-8")));
        req.setParam(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
        return req;
      }

      case UpdateLog.DELETE_BY_QUERY: {
        String query = (String) entry.get(2);
        req.deleteByQuery(query);
        req.setParam(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
        return req;
      }

      case UpdateLog.COMMIT: {
        return null;
      }

      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown Operation! " + oper);
    }
  }

  /**
   * Exception to catch update request issues with the target cluster.
   */
  public class CdcReplicatorException extends Exception {

    private final UpdateRequest req;
    private final UpdateResponse rsp;

    public CdcReplicatorException(UpdateRequest req, UpdateResponse rsp) {
      this.req = req;
      this.rsp = rsp;
    }

  }

}
