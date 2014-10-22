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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcReplicator implements Runnable {

  private final CdcReplicatorState state;

  private static final int BATCH_SIZE = 32;

  protected static Logger log = LoggerFactory.getLogger(CdcReplicator.class);

  public CdcReplicator(CdcReplicatorState state) {
    this.state = state;
  }

  @Override
  public void run() {
    CdcrUpdateLog.CdcrLogReader logReader = this.state.getLogReader();

    // Make a clone to be able to reset the original position of the reader in case of errors
    CdcrUpdateLog.CdcrLogReader subReader = logReader.getSubReader();

    try {
      // create batch update request
      UpdateRequest req = new UpdateRequest();

      Object o = subReader.next();
      for (int i = 0; i < BATCH_SIZE && o != null; i++, o = subReader.next()) {
        if (this.processUpdate(o, req) != null) {
          UpdateResponse rsp = req.process(state.getClient());
          if (rsp.getStatus() != 0) {
            throw new CdcReplicatorException(req, rsp);
          }
          // reset the number of consecutive errors
          state.resetConsecutiveErrors();
        }

        // the update has been forwarded successfully, update log reader position
        // TODO: not very efficient as we create a new log reader for each update ...
        logReader.forwardSeek(subReader);
      }
    }
    catch (InterruptedException | IOException e) {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcReplicatorState.ErrorType.INTERNAL);
    }
    catch (SolrServerException e) {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcReplicatorState.ErrorType.SERVER);
    }
    catch (CloudSolrServer.RouteException e) {
      log.warn("Failed to forward update request", e);
      state.reportError(CdcReplicatorState.ErrorType.BAD_REQUEST);
    }
    catch (CdcReplicatorException e) {
      log.warn("Failed to forward update request {}. Got response {}", e.req, e.rsp);
      state.reportError(CdcReplicatorState.ErrorType.BAD_REQUEST);
    }
    finally {
      subReader.close();
    }
  }

  private UpdateRequest processUpdate(Object o, UpdateRequest req) {
    req.clear();

    // should currently be a List<Oper,Ver,Doc/Id>
    List entry = (List) o;

    int operationAndFlags = (Integer) entry.get(0);
    int oper = operationAndFlags & UpdateLog.OPERATION_MASK;
    long version = (Long) entry.get(1);

    switch (oper) {
      case UpdateLog.ADD: {
        SolrInputDocument sdoc = (SolrInputDocument) entry.get(entry.size() - 1);
        req.add(sdoc);
        req.setParam(DistributedUpdateProcessor.VERSION_FIELD, Long.toString(version));
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
