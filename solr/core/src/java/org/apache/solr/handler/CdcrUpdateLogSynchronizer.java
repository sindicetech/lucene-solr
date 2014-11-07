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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcrUpdateLogSynchronizer implements CdcrStateManager.CdcrStateObserver {

  private CdcrLeaderStateManager leaderStateManager;
  private ScheduledExecutorService scheduler;

  private final SolrCore core;
  private final String collection;
  private final String shardId;
  private final String path;

  protected static Logger log = LoggerFactory.getLogger(CdcrUpdateLogSynchronizer.class);

  CdcrUpdateLogSynchronizer(SolrCore core, String path) {
    this.core = core;
    this.path = path;
    this.collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.shardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  void setLeaderStateManager(final CdcrLeaderStateManager leaderStateManager) {
    this.leaderStateManager = leaderStateManager;
    this.leaderStateManager.register(this);
  }

  @Override
  public void stateUpdate() {
    // If I am not the leader, I need to synchronise periodically my update log with my leader.
    if (!leaderStateManager.amILeader()) {
      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleWithFixedDelay(new UpdateLogSynchronisation(), 1000, 1000, TimeUnit.MILLISECONDS);
      return;
    }

    this.shutdown();
  }

  void shutdown() {
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  private class UpdateLogSynchronisation implements Runnable {

    private String getLeaderUrl() {
      ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
      ClusterState cstate = zkController.getClusterState();
      ZkNodeProps leaderProps = cstate.getLeader(collection, shardId);
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
      return nodeProps.getCoreUrl();
    }

    @Override
    public void run() {
      String leaderUrl = getLeaderUrl();

      HttpSolrServer server = new HttpSolrServer(leaderUrl);
      server.setConnectionTimeout(15000);
      server.setSoTimeout(60000);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.ACTION, CdcrRequestHandler.CdcrAction.LASTPROCESSEDVERSION.toString());

      SolrRequest request = new QueryRequest(params);
      request.setPath(path);

      long lastVersion;
      try {
        NamedList response = server.request(request);
        lastVersion = (Long) response.get("lastProcessedVersion"); // TODO: register this param somewhere
        log.debug("My leader {} says its last processed _version_ number is: {}. I am {}", leaderUrl, lastVersion,
            core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
      }
      catch(IOException | SolrServerException e) {
        log.warn("Couldn't get last processed version from leader {}: {}", leaderUrl, e.getMessage());
        return;
      }
      finally {
        server.shutdown();
      }

      // if we received -1, it means that the log reader on the leader has not yet started to read log entries
      // do nothing
      if (lastVersion == -1) {
        return;
      }

      CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();

      try {
        log.debug("Advancing replica buffering tlog reader to {} @ {}:{}", lastVersion, collection, shardId);
        ulog.getBufferToggle().seek(lastVersion);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Couldn't advance replica buffering tlog reader to {} (to remove old tlogs): {}", lastVersion, e.getMessage());
      }
      catch (IOException e) {
        log.warn("Couldn't advance replica buffering tlog reader to {} (to remove old tlogs): {}", lastVersion, e.getMessage());
      }
    }
  }

}
