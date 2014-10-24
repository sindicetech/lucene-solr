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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.update.CdcrUpdateLog;

class CdcReplicatorState {

  private final String targetCollection;
  private final CloudSolrServer targetClient;

  private CdcrUpdateLog.CdcrLogReader logReader;

  private long consecutiveErrors = 0;
  private final Map<ErrorType, Long> errorCounters = new HashMap<>();

  CdcReplicatorState(final String targetCollection, final CloudSolrServer targetClient) {
    this.targetCollection = targetCollection;
    this.targetClient = targetClient;
  }

  /**
   * Initialise the replicator state with a {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}
   * that is positioned at the last target cluster checkpoint.
   */
  void init(final CdcrUpdateLog.CdcrLogReader logReader) {
    this.logReader = logReader;
  }

  void closeLogReader() {
    if (logReader != null) {
      logReader.close();
      logReader = null;
    }
  }

  CdcrUpdateLog.CdcrLogReader getLogReader() {
    return logReader;
  }

  String getTargetCollection() {
    return targetCollection;
  }

  CloudSolrServer getClient() {
    return targetClient;
  }

  void shutdown() {
    targetClient.shutdown();
    this.closeLogReader();
  }

  void reportError(ErrorType error) {
    if (!errorCounters.containsKey(error)) {
      errorCounters.put(error, 0l);
    }
    errorCounters.put(error, errorCounters.get(error) + 1);
    consecutiveErrors++;
  }

  void resetConsecutiveErrors() {
    consecutiveErrors = 0;
  }

  long getConsecutiveErrors() {
    return consecutiveErrors;
  }

  enum ErrorType {
    INTERNAL,
    BAD_REQUEST
  }

}
