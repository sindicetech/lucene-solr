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

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcrLeaderStateManager {

  private boolean isInitialised = false;

  private boolean amILeader = false;

  private Watcher watcher;

  private SolrCore core;

  private CdcReplicatorStatesManager replicatorStatesManager;
  private CdcReplicatorScheduler replicatorScheduler;

  protected static Logger log = LoggerFactory.getLogger(CdcrProcessStateManager.class);

  /**
   * Register the {@link org.apache.solr.handler.CdcReplicatorStatesManager} to notify it
   * from a leader state change.
   */
  void register(CdcReplicatorStatesManager replicatorStatesManager) {
    this.replicatorStatesManager = replicatorStatesManager;
  }

  /**
   * Register the {@link org.apache.solr.handler.CdcReplicatorScheduler} to notify it
   * from a process state change.
   */
  void register(CdcReplicatorScheduler replicatorScheduler) {
    this.replicatorScheduler = replicatorScheduler;
  }


  void init() {
    if (!isInitialised) {
      // Fetch leader state and register the watcher at startup
      try {
        this.checkIfIAmLeader();
        SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
        watcher = this.initWatcher(zkClient);
        zkClient.getData(this.getZnodePath(), watcher, null, true);
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed fetching initial leader state and setting watch", e);
      }
      isInitialised = true;
    }
  }

  private Watcher initWatcher(SolrZkClient zkClient) {
    LeaderStateWatcher watcher = new LeaderStateWatcher();
    return zkClient.wrapWatcher(watcher);
  }

  /**
   * We cannot initialise the {@link org.apache.solr.handler.CdcrLeaderStateManager}
   * in {@link org.apache.solr.util.plugin.SolrCoreAware#inform(org.apache.solr.core.SolrCore)}
   * since at startup, the core is not yet fully instantiated and therefore the leader process has not yet been
   * registered. Therefore, we initialise it only when the process state changes.
   */
  public void inform(CdcrRequestHandler.ProcessState state) {
    this.init();
  }

  public void inform(SolrCore core) {
    this.core = core;
    // No need to reinitialise everything if the core is reloaded
  }

  private void checkIfIAmLeader() throws KeeperException, InterruptedException {
    String myShardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    String myCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    ZkStateReader zkStateReader = core.getCoreDescriptor().getCoreContainer().getZkController().getZkStateReader();
    zkStateReader.updateClusterState(true); // force a cluster state update
    Replica myLeader = zkStateReader.getLeaderRetry(myCollection, myShardId);
    this.setAmILeader(myLeader.getName().equals(core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName()));
  }

  private String getZnodePath() {
    String myShardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    String myCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    return "/collections/" + myCollection + "/leaders/" + myShardId;
  }

  void setAmILeader(boolean amILeader) {
    if (this.amILeader != amILeader) {
      this.amILeader = amILeader;
      // notify the replicator states manager
      this.replicatorStatesManager.inform(amILeader);
      // notify the scheduler
      this.replicatorScheduler.inform(amILeader);
    }
  }

  boolean amILeader() {
    return amILeader;
  }

  private class LeaderStateWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      log.debug("The leader state has changed: {}", event);
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      try {
        // we will receive a NodeDeleted event during leader election,
        // but checkIfIAmLeader will block until the node is created again
        CdcrLeaderStateManager.this.checkIfIAmLeader();
        SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
        zkClient.getData(CdcrLeaderStateManager.this.getZnodePath(), watcher, null, true);

        String coreName = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
        String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
        log.info("Received new leader state @ {} {}", coreName, shard);
      }
      catch (KeeperException | InterruptedException e) {
        String coreName = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
        String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
        log.warn("Failed updating leader state and setting watch @ " + coreName + " " + shard, e);
      }
    }

  }

}
