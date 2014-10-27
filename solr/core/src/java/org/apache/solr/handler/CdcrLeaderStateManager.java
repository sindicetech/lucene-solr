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

/**
 * <p>
 *   Manage the leader state of the CDCR nodes.
 * </p>
 * <p>
 *   It takes care of notifying the {@link CdcReplicatorManager} in case
 *   of a leader state change.
 * </p>
 */
class CdcrLeaderStateManager {

  private boolean isInitialised = false;

  private boolean amILeader = false;

  private LeaderStateWatcher wrappedWatcher;
  private Watcher watcher;

  private SolrCore core;

  protected static Logger log = LoggerFactory.getLogger(CdcrProcessStateManager.class);

  CdcrLeaderStateManager(final SolrCore core) {
    this.core = core;

    // Fetch leader state and register the watcher at startup
    try {
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      // if the node does not exist, it means that the leader was not yet registered and the call to
      // checkIfIAmLeader will fail with a timeout. This can happen
      // when the cluster is starting up. The core is not yet fully loaded, and the leader election process
      // is waiting for it.
      watcher = this.initWatcher(zkClient);
      if (zkClient.exists(this.getZnodePath(), watcher, true) != null) {
        this.checkIfIAmLeader();
        zkClient.getData(this.getZnodePath(), watcher, null, true);
      }
    }
    catch (KeeperException | InterruptedException e) {
      log.warn("Failed fetching initial leader state and setting watch", e);
    }
  }

  /**
   * SolrZkClient does not guarantee that a watch object will only be triggered once for a given notification
   * if we does not wrap the watcher - see SOLR-6621.
   */
  private Watcher initWatcher(SolrZkClient zkClient) {
    wrappedWatcher = new LeaderStateWatcher();
    return zkClient.wrapWatcher(wrappedWatcher);
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
      this.callback(); // notify the observers of a state change
    }
  }

  boolean amILeader() {
    return amILeader;
  }

  void shutdown() {
    if (wrappedWatcher != null) {
      wrappedWatcher.cancel(); // cancel the watcher to avoid spurious warn messages during shutdown
    }
  }

  private class LeaderStateWatcher implements Watcher {

    private boolean isCancelled = false;

    /**
     * Cancel the watcher to avoid spurious warn messages during shutdown.
     */
    void cancel() {
      isCancelled = true;
    }

    @Override
    public void process(WatchedEvent event) {
      if (isCancelled) return; // if the watcher is cancelled, do nothing.
      String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
      String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();

      log.debug("The leader state has changed: {} @ {}:{}", event, collectionName, shard);
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      try {
        log.info("Received new leader state @ {}:{}", collectionName, shard);
        // we will receive a NodeDeleted event during leader election,
        // but checkIfIAmLeader will block until the node is created again
        CdcrLeaderStateManager.this.checkIfIAmLeader();
        SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
        zkClient.getData(CdcrLeaderStateManager.this.getZnodePath(), watcher, null, true);
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed updating leader state and setting watch @ " + collectionName + ":" + shard, e);
      }
    }

  }

  private CdcReplicatorManager replicatorManager;

  void register(CdcReplicatorManager replicatorManager) {
    this.replicatorManager = replicatorManager;
  }

  /**
   * Notify the {@link org.apache.solr.handler.CdcReplicatorManager} of a state change.
   */
  private void callback() {
    if (replicatorManager != null) {
      this.replicatorManager.stateUpdate();
    }
  }

}
