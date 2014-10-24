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
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage the state of the update log buffer. It is responsible of synchronising the state
 * through Zookeeper. The state of the buffer is stored in the zk node defined by {@link #getZnodePath()}.
 */
class CdcrBufferStateManager {

  private Watcher watcher;

  private SolrCore core;

  static CdcrRequestHandler.BufferState DEFAULT_STATE = CdcrRequestHandler.BufferState.ENABLED;

  protected static Logger log = LoggerFactory.getLogger(CdcrBufferStateManager.class);

  CdcrBufferStateManager(final SolrCore core) {
    this.core = core;

    // Ensure that the state znode exists
    this.createStateNode();

    // set default state
    this.setState(DEFAULT_STATE);

    // Startup and register the watcher at startup
    try {
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
     this.initWatcher(zkClient);
      this.setState(CdcrRequestHandler.BufferState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
    }
    catch (KeeperException | InterruptedException e) {
      log.warn("Failed fetching initial state", e);
    }
  }

  /**
   * SolrZkClient does not guarantee that a watch object will only be triggered once for a given notification
   * if we does not wrap the watcher - see SOLR-6621.
   */
  private void initWatcher(SolrZkClient zkClient) {
    BufferStateWatcher watcher = new BufferStateWatcher();
    this.watcher = zkClient.wrapWatcher(watcher);
  }

  private String getZnodeBase() {
    return "/collections/" + core.getCoreDescriptor().getCloudDescriptor().getCollectionName() + "/cdcr/state";
  }

  private String getZnodePath() {
    return getZnodeBase() + "/buffer";
  }

  void setState(CdcrRequestHandler.BufferState state) {
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
    switch (state) {
      case ENABLED: {
        ulog.enableBuffer();
        return;
      }
      case DISABLED: {
        ulog.disableBuffer();
        return;
      }
    }
  }

  CdcrRequestHandler.BufferState getState() {
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
    return ulog.isBuffering() ? CdcrRequestHandler.BufferState.ENABLED : CdcrRequestHandler.BufferState.DISABLED;
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
      this.setState(CdcrRequestHandler.BufferState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
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
        zkClient.create(this.getZnodePath(), DEFAULT_STATE.getBytes(), CreateMode.PERSISTENT, true);
        log.info("Created znode {}", this.getZnodePath());
      }
    }
    catch (KeeperException | InterruptedException e) {
      log.warn("Failed to create CDCR buffer state node", e);
    }
  }

  /**
   * TODO: Should we handle disconnection and expired sessions ?
   */
  private class BufferStateWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
      String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();

      log.debug("The CDCR buffer state has changed: {} @ {}:{}", event, collectionName, shard);
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      try {
        CdcrRequestHandler.BufferState state = CdcrRequestHandler.BufferState.get(zkClient.getData(CdcrBufferStateManager.this.getZnodePath(), watcher, null, true));
        log.info("Received new CDCR buffer state from watcher: {} @ {}:{}", state, collectionName, shard);
        CdcrBufferStateManager.this.setState(state);
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed synchronising new state @ " + collectionName + ":" + shard, e);
      }
    }

  }

}
