/**
 * Copyright (c) 2014 Renaud Delbru. All Rights Reserved.
 */
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

import java.util.Locale;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrRequestHandler extends RequestHandlerBase  implements SolrCoreAware {

  protected static Logger log = LoggerFactory.getLogger(CdcrRequestHandler.class);

  private SolrCore core;
  private String collection;

  private CdcrStateManager stateManager;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure that the core is ZKAware
    if(!core.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Pick the action
    SolrParams params = req.getParams();
    CdcrAction action = null;
    String a = params.get(CommonParams.ACTION);
    if (a != null) {
      action = CdcrAction.get(a);
    }
    if (action == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
    }

    switch (action) {
      case START: {
        this.handleStartAction(req, rsp);
        break;
      }
      case STOP: {
        this.handleStopAction(req, rsp);
        break;
      }
      case STATUS: {
        this.handleStatusAction(req, rsp);
        break;
      }
      default: {
        throw new RuntimeException("Unknown action: " + action);
      }
    }

    rsp.setHttpCaching(false);
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    collection = this.core.getName();

    // Create the state manager after having a reference to the core and knowing our collection
    if (stateManager == null) {
      stateManager = new CdcrStateManager();
    }
  }

  private void handleStartAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (stateManager.getState() == CdcrState.STOPPED) {
      stateManager.setState(CdcrState.STARTED);
      stateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), stateManager.getState().toLower());
  }

  private void handleStopAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (stateManager.getState() == CdcrState.STARTED) {
      stateManager.setState(CdcrState.STOPPED);
      stateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), stateManager.getState().toLower());
  }

  private void handleStatusAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add(CdcrAction.STATUS.toLower(), stateManager.getState().toLower());
  }

  private boolean amILeader() {
    try {
      ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
      ClusterState cstate = zkController.getClusterState();
      DocCollection coll = cstate.getCollection(collection);
      String myShardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
      Slice mySlice = coll.getSlice(myShardId);
      Replica myLeader = zkController.getZkStateReader().getLeaderRetry(collection, myShardId);
      return myLeader.getName().equals(core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
    }
    catch (InterruptedException e) {
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
  }

  @Override
  public String getDescription() {
    return "Manage Cross Data Center Replication";
  }

  /**
   * The actions supported by the CDCR API
   */
  public enum CdcrAction {
    START,
    STOP,
    STATUS;

    public static CdcrAction get(String p) {
      if (p != null) {
        try {
          return CdcrAction.valueOf(p.toUpperCase(Locale.ROOT));
        }
        catch (Exception e) {}
      }
      return null;
    }

    public String toLower(){
      return toString().toLowerCase(Locale.ROOT);
    }

  }

  /**
   * The possible states of the CDCR
   */
  public enum CdcrState {
    STARTED,
    STOPPED;

    public static CdcrState get(byte[] state) {
      if (state != null) {
        try {
          return CdcrState.valueOf(new String(state).toUpperCase(Locale.ROOT));
        }
        catch (Exception e) {}
      }
      return null;
    }

    public String toLower(){
      return toString().toLowerCase(Locale.ROOT);
    }

    public byte[] getBytes() {
      return toLower().getBytes();
    }

  }

  /**
   * Manage the life-cycle state of the CDCR. It is responsible of synchronising the state
   * through Zookeeper. The state of the CDCR is stored in the zk node defined by {@link #getZnodePath()}.
   */
  private class CdcrStateManager {

    private CdcrState state;

    private CdcrStatusWatcher watcher = new CdcrStatusWatcher();

    CdcrStateManager() {
      // Ensure that the status znode exists
      this.createStatusNode();

      // Register the watcher
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      try {
        state = CdcrState.get(zkClient.getData(this.getZnodePath(), watcher, null, true));
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed fetching initial status", e);
      }
    }

    CdcrState getState() {
      return state;
    }

    private String getZnodeBase() {
      return "/collections/" + collection + "/cdcr";
    }

    private String getZnodePath() {
      return getZnodeBase() + "/status";
    }

    void setState(CdcrState state) {
      this.state = state;
    }

    /**
     * Synchronise the state to Zookeeper. This method must be called only by the handler receiving the
     * action.
     */
    void synchronize() {
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      try {
        zkClient.setData(this.getZnodePath(), state.getBytes(), true);
        // check if nobody changed it in the meantime, and set a new watcher
        state = CdcrState.get(zkClient.getData(this.getZnodePath(), watcher, null, true));
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed synchronising new status", e);
      }
    }

    private void createStatusNode() {
      SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
      try {
        if (!zkClient.exists(this.getZnodePath(), true)) {
          if (!zkClient.exists(this.getZnodeBase(), true)) {
            zkClient.makePath(this.getZnodeBase(), CreateMode.PERSISTENT, true);
          }
          zkClient.create(this.getZnodePath(), CdcrState.STOPPED.getBytes(), CreateMode.PERSISTENT, true);
          log.info("Created znode {}", this.getZnodePath());
        }
      }
      catch (KeeperException | InterruptedException e) {
        log.warn("Failed to create CDCR status node", e);
      }
    }

    /**
     * TODO: Should we handle disconnection and expired sessions ?
     */
    private class CdcrStatusWatcher implements Watcher {

      @Override
      public void process(WatchedEvent event) {
        log.debug("A CDCR status change: {}", event);
        if (Event.EventType.None.equals(event.getType())) {
          return;
        }
        SolrZkClient zkClient = core.getCoreDescriptor().getCoreContainer().getZkController().getZkClient();
        try {
          state = CdcrState.get(zkClient.getData(CdcrStateManager.this.getZnodePath(), watcher, null, true));
          log.info("Received new CDCR state from watcher: {}", state);
        }
        catch (KeeperException | InterruptedException e) {
          log.warn("Failed synchronising new status", e);
        }
      }

    }

  }

}
