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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;

class CdcReplicatorStatesManager {

  private boolean isInitialised = false;

  private List<CdcReplicatorState> replicatorStates;

  private SolrCore core;

  private Map<String,List<SolrParams>> replicasConfiguration;

  private CdcrRequestHandler.ProcessState processState = CdcrProcessStateManager.DEFAULT_STATE;
  private boolean amILeader = false;

  void setReplicasConfiguration(Map<String,List<SolrParams>> replicasConfiguration) {
    this.replicasConfiguration = replicasConfiguration;
  }

  void init() {
    if (!isInitialised) {
      // create states
      replicatorStates = new ArrayList<>();
      String myCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
      List<SolrParams> targets = replicasConfiguration.get(myCollection);
      if (targets != null) {
        for (SolrParams params : targets) {
          String zkHost = params.get(CdcrRequestHandler.ZK_HOST_PARAM);
          String targetCollection = params.get(CdcrRequestHandler.TARGET_COLLECTION_PARAM);

          CloudSolrServer client = new CloudSolrServer(zkHost, true);
          client.setDefaultCollection(targetCollection);
          replicatorStates.add(new CdcReplicatorState(client));
        }
      }
      isInitialised = true;
    }
  }

  /**
   * If we become the leader and the process state is STARTED, we need to initialise the log readers.
   * Otherwise, if the process state is STOPPED pr if we are not the leader, we need to close the log readers.
   */
  void inform(boolean amILeader) {
    this.amILeader = amILeader;
    if (amILeader && processState.equals(CdcrRequestHandler.ProcessState.STARTED)) {
      this.initLogReaders();
      return;
    }
    this.closeLogReaders();
  }

  /**
   * If the process state changes to STARTED, and we are the leader, we need to initialise the log readers. Otherwise,
   * we need to close the log readers.
   */
  void inform(CdcrRequestHandler.ProcessState state) {
    this.processState = state;
    if (amILeader && processState.equals(CdcrRequestHandler.ProcessState.STARTED)) {
      this.initLogReaders();
      return;
    }
    this.closeLogReaders();
  }

  public void inform(SolrCore core) {
    this.core = core;

    // if the core is reloaded, we need to re-initialise the log readers since
    // they belong to an old UpdateLog instance
    if (amILeader && processState.equals(CdcrRequestHandler.ProcessState.STARTED)) {
      this.initLogReaders();
    }
  }

  /**
   * Shutdown all the {@link org.apache.solr.handler.CdcReplicatorState} by closing their
   * {@link org.apache.solr.client.solrj.impl.CloudSolrServer} and
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}.
   * TODO: Where and when to call it ?
   */
  void shutdown() {
    for (CdcReplicatorState state : replicatorStates) {
      state.shutdown();
    }
    replicatorStates.clear();
  }

  void closeLogReaders() {
    for (CdcReplicatorState state : replicatorStates) {
      state.closeLogReader();
    }
  }

  void initLogReaders() {
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
    for (CdcReplicatorState state : replicatorStates) {
      state.closeLogReader();
      // TODO: proper initialisation by fetching last checkpoint from target cluster
      state.init(ulog.newLogReader());
    }
  }

  List<CdcReplicatorState> getReplicatorStates() {
    return replicatorStates;
  }

}
