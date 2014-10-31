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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcrNonLeaderScheduler {
  private final String collection;
  private final String shardId;
  private final SolrCore core;
  private boolean isStarted = false;
  private static final int ZK_TIMEOUT = 5000;

  private ScheduledExecutorService scheduler;

  protected static Logger log = LoggerFactory.getLogger(CdcrNonLeaderScheduler.class);

  private String getLeaderUrl() throws KeeperException, InterruptedException {
    ZkStateReader zkStateReader = core.getCoreDescriptor().getCoreContainer().getZkController().getZkStateReader();
    zkStateReader.updateClusterState(true); // force a cluster state update
    return zkStateReader.getLeaderUrl(collection, shardId, ZK_TIMEOUT);
  }

  private String getLeaderUrl2() {
    ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
    ClusterState cstate = zkController.getClusterState();
    ZkNodeProps leaderProps = cstate.getLeader(collection, shardId);
    ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
    return nodeProps.getCoreUrl();
  }

  CdcrNonLeaderScheduler(SolrCore core) {
    this.core = core;
    this.collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.shardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  void start() {
    log.info("CdcrNonLeaderScheduler starting");
    if (!isStarted) {
      scheduler = Executors.newSingleThreadScheduledExecutor();

      scheduler.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          log.info("CdcrNonLeaderScheduler invoked...");

          String leaderUrl = null;
//          try {
            leaderUrl = getLeaderUrl2();
//          } catch (InterruptedException | KeeperException e) {
//            log.warn("Couldn't determine leader URL for collection {} shard {}: {}", collection, shardId, e.getMessage());
//            return;
//          }

          HttpSolrServer server = new HttpSolrServer(leaderUrl);
          server.setConnectionTimeout(15000);
          server.setSoTimeout(60000);

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CommonParams.ACTION, CdcrRequestHandler.CdcrAction.LEADERPROCESSEDVERSION.toString());

          SolrRequest request = new QueryRequest(params);
          request.setPath("/cdcr"); //TODO: hardcoded reference to cdcr path

          long lastVersion;
          try {
            NamedList response = server.request(request);
            lastVersion = (Long) response.get("lastProcessedVersion");
            log.info("My leader {} says its last processed _version_ number is: {}. I am {}", leaderUrl, lastVersion,
                core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
          } catch(IOException | SolrServerException ex) {
            log.warn("Couldn't get last processed version number from leader " + leaderUrl + ": " + ex.getMessage());
            return;
          } finally {
            server.shutdown();
          }

          CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();

          try {
            log.info("Advancing replica buffering tlog reader to {}", lastVersion);
            ulog.getBufferToggle().seek(lastVersion);
          } catch (IOException | InterruptedException e) {
            log.warn("Couldn't advance replica buffering tlog reader to {} (to remove old tlogs): {}", lastVersion, e.getMessage());
          }
        }

      }, 1000, 1000, TimeUnit.MILLISECONDS);
      isStarted = true;
    }
  }

  void shutdown() {
    log.info("CdcrNonLeaderScheduler stopping");
    if (isStarted) {
        scheduler.shutdownNow();
        isStarted = false;
    }
  }
}
