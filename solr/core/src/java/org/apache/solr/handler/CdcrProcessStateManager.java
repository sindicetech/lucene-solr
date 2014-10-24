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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 *   Manage the life-cycle state of the CDCR. It is responsible of synchronising the state
 *   through Zookeeper. The state of the CDCR is stored in the zk node defined by {@link #getZnodePath()}.
 * </p>
 * <p>
 *   It takes care of notifying the {@link org.apache.solr.handler.CdcReplicatorStatesManager} and the
 *   {@link org.apache.solr.handler.CdcrLeaderStateManager} in case
 *   of a process state change.
 * </p>
 */
class CdcrProcessStateManager {

  private boolean isInitialised = false;

  private CdcrRequestHandler.ProcessState state = DEFAULT_STATE;

  private ProcessStateWatcher watcher = new ProcessStateWatcher();

  private SolrCore core;

  private CdcReplicatorStatesManager replicatorStatesManager;
  private CdcrLeaderStateManager leaderStateManager;
  private CdcReplicatorScheduler replicatorScheduler;

  /**
   * The default state must be STOPPED. See comments in
   * {@link #setState(org.apache.solr.handler.CdcrRequestHandler.ProcessState)}.
   */
  static CdcrRequestHandler.ProcessState DEFAULT_STATE = CdcrRequestHandler.ProcessState.STOPPED;

  protected static Logger log = LoggerFactory.getLogger(CdcrProcessStateManager.class);

  /**
   * Register the {@link org.apache.solr.handler.CdcReplicatorStatesManager} to notify it
   * from a process state change.
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

  /**
   * Register the {@link org.apache.solr.handler.CdcrLeaderStateManager} to notify it
   * from a process state change.
   */
  void register(CdcrLeaderStateManager leaderStateManager) {
    this.leaderStateManager = leaderStateManager;
  }

  void init() {
    if (!isInitialised) {
      // Ensure that the status znode exists
      this.createStateNode();

      // Register the watcher at startup
      try {
        SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
        this.setState(CdcrRequestHandler.ProcessState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed fetching initial state", e);
      }

      isInitialised = true;
    }
  }

  public void inform(SolrCore core) {
    this.core = core;
    // No need to reinitialise anything if the core is reloaded
  }

  private String getZnodeBase() {
    return "/collections/" + core.getCoreDescriptor().getCloudDescriptor().getCollectionName() + "/cdcr/state";
  }

  private String getZnodePath() {
    return getZnodeBase() + "/process";
  }

  void setState(CdcrRequestHandler.ProcessState state) {
    // somehow a tricky logic is happening here. The first time the handler is instantiated, we will received a
    // state from #init, which should be identical to the default state. In that case, the following condition will be
    // false, and the leaderStateManager.inform(state) will not be called. This will avoid to get a Solr exception
    // when the leader state manager tries to get the leader, since there is not yet a registered leader.
    // Later, if the handler is instantiated again, e.g., when a node is restarted, then it should be fine, since
    // a leader will probably be registered.
    // TODO: See if we can improve this.
    if (this.state != state) {
      this.state = state;
      // notify first the leader state manager since it might not yet been initialised - the
      // replicator states manager relies on it
      this.leaderStateManager.inform(state);
      // notify the replicator states manager
      this.replicatorStatesManager.inform(state);
      // notify the scheduler
      this.replicatorScheduler.inform(state);
    }
  }

  CdcrRequestHandler.ProcessState getState() {
    return state;
  }

  /**
   * Synchronise the state to Zookeeper. This method must be called only by the handler receiving the
   * action.
   */
  void synchronize() {
    SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
    try {
      zkClient.setData(this.getZnodePath(), this.getState().getBytes(), true);
      // check if nobody changed it in the meantime, and set a new watcher
      this.setState(CdcrRequestHandler.ProcessState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
    }
    catch (KeeperException | InterruptedException e) {
      log.warn("Failed synchronising new state", e);
    }
  }

  private void createStateNode() {
    SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
    try {
      if (!zkClient.exists(this.getZnodePath(), true)) {
        if (!zkClient.exists(this.getZnodeBase(), true)) {
          zkClient.makePath(this.getZnodeBase(), CreateMode.PERSISTENT, true);
        }
        zkClient.create(this.getZnodePath(), CdcrRequestHandler.ProcessState.STOPPED.getBytes(), CreateMode.PERSISTENT, true);
        log.info("Created znode {}", this.getZnodePath());
      }
    }
    catch (KeeperException | InterruptedException e) {
      log.warn("Failed to create CDCR process state node", e);
    }
  }

  /**
   * TODO: Should we handle disconnection and expired sessions ?
   */
  private class ProcessStateWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      log.debug("The CDCR process state has changed: {}", event);
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      try {
        CdcrRequestHandler.ProcessState state = CdcrRequestHandler.ProcessState.get(zkClient.getData(CdcrProcessStateManager.this.getZnodePath(), watcher, null, true));
        CdcrProcessStateManager.this.setState(state);
        log.info("Received new CDCR process state from watcher: {}", state);
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed synchronising new state", e);
      }
    }

  }

}
