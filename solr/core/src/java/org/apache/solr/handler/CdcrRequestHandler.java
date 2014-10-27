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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcrRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  protected static Logger log = LoggerFactory.getLogger(CdcrRequestHandler.class);

  private SolrCore core;
  private String collection;

  private Map<String,List<SolrParams>> replicasConfiguration;

  private CdcrProcessStateManager processStateManager;
  private CdcrBufferStateManager bufferStateManager;
  private CdcReplicatorManager replicatorManager;
  private CdcrLeaderStateManager leaderStateManager;

  public static final String REPLICAS_PARAM = "replicas";
  public static final String SOURCE_COLLECTION_PARAM = "source";
  public static final String TARGET_COLLECTION_PARAM = "target";
  public static final String ZK_HOST_PARAM = "zkHost";

  @Override
  public void init(NamedList args) {
    super.init(args);

    replicasConfiguration = new HashMap<>();

    if (args != null) {
      List replicas = args.getAll(REPLICAS_PARAM);
      for (Object replica : replicas) {
        if (replicas != null && replica instanceof NamedList) {
          SolrParams params = SolrParams.toSolrParams((NamedList) replica);
          if (!replicasConfiguration.containsKey(params.get(SOURCE_COLLECTION_PARAM))) {
            replicasConfiguration.put(params.get(SOURCE_COLLECTION_PARAM), new ArrayList<SolrParams>());
          }
          replicasConfiguration.get(params.get(SOURCE_COLLECTION_PARAM)).add(params);
        }
      }
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
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
      case COLLECTIONCHECKPOINT: {
        this.handleCollectionCheckpointAction(req, rsp);
        break;
      }
      case SLICECHECKPOINT: {
        this.handleSliceCheckpointAction(req, rsp);
        break;
      }
      case ENABLEBUFFER: {
        this.handleEnableBufferAction(req, rsp);
        break;
      }
      case DISABLEBUFFER: {
        this.handleDisableBufferAction(req, rsp);
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
    collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();

    // Make sure that the core is ZKAware
    if(!core.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Make sure that the core is using the CdcrUpdateLog implementation
    if(!(core.getUpdateHandler().getUpdateLog() instanceof CdcrUpdateLog)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Solr instance is not configured with the cdcr update log.");
    }

    // Initialisation phase
    // If the Solr cloud is being initialised, each CDCR node will start up in its default state, i.e., STOPPED
    // and non-leader. The leader state will be updated later, when all the Solr cores have been loaded.
    // If the Solr cloud has already been initialised, and the core is reloaded (i.e., because a node died or a new node
    // is added to the cluster), each CDCR node will synchronise its state with the global CDCR state that is stored
    // in zookeeper.

    // Initialise the buffer state manager
    bufferStateManager = new CdcrBufferStateManager(core);
    // Initialise the process state manager
    processStateManager = new CdcrProcessStateManager(core);
    // Initialise the leader state manager
    leaderStateManager = new CdcrLeaderStateManager(core);
    // Initialise the replicator states manager
    replicatorManager = new CdcReplicatorManager(core, replicasConfiguration);
    replicatorManager.setProcessStateManager(processStateManager);
    replicatorManager.setLeaderStateManager(leaderStateManager);
    // we need to inform it of a state event since the process and leader state
    // may have been synchronised during the initialisation
    replicatorManager.stateUpdate();

    // register the close hook
    this.registerCloseHook(core);
  }

  /**
   * register a close hook to properly shutdown the state manager and scheduler
   */
  private void registerCloseHook(SolrCore core) {
    core.addCloseHook(new CloseHook() {

      @Override
      public void preClose(SolrCore core) {
        replicatorManager.shutdown();
        leaderStateManager.shutdown();
      }

      @Override
      public void postClose(SolrCore core) {}

    });
  }

  /**
   * <p>
   *   Update and synchronize the process state.
   * </p>
   * <p>
   *   The process state manager must notify the replicator states manager of the change of state.
   * </p>
   */
  private void handleStartAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (processStateManager.getState() == ProcessState.STOPPED) {
      processStateManager.setState(ProcessState.STARTED);
      processStateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleStopAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (processStateManager.getState() == ProcessState.STARTED) {
      processStateManager.setState(ProcessState.STOPPED);
      processStateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleStatusAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add(CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private NamedList getStatus() {
    NamedList status = new NamedList();
    status.add(ProcessState.getParam(), processStateManager.getState().toLower());
    status.add(BufferState.getParam(), bufferStateManager.getState().toLower());
    return status;
  }

  /**
   * This action is generally executed on the target cluster in order to retrieve the latest update checkpoint.
   * This checkpoint is used on the source cluster to setup the
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader} of a slice leader. <br/>
   * This method will execute in parallel one
   * {@link org.apache.solr.handler.CdcrRequestHandler.CdcrAction#SLICECHECKPOINT} request per slice leader. It will
   * then pick the lowest version number as checkpoint. Picking the lowest amongst all slices will ensure that we do not
   * pick a checkpoint that is ahead of the source cluster. This can occur when other slice leaders are sending new
   * updates to the target cluster while we are currently instantiating the
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}.
   * This solution only works in scenarios where the topology of the source and target clusters are identical.
   */
  private void handleCollectionCheckpointAction(SolrQueryRequest req, SolrQueryResponse rsp)
  throws IOException, SolrServerException {
    ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
    ClusterState cstate = zkController.getClusterState();
    Collection<Slice> slices = cstate.getActiveSlices(collection);

    ExecutorService parallelExecutor = Executors.newCachedThreadPool(new DefaultSolrThreadFactory("parallelCdcrExecutor"));
    String cdcrPath = rsp.getToLog().get("path").toString();

    List<Callable<Long>> callables = new ArrayList<>();
    for (Slice slice : slices) {
      ZkNodeProps leaderProps = cstate.getLeader(collection, slice.getName());
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
      callables.add(new SliceCheckpointCallable(nodeProps.getCoreUrl(), cdcrPath));
    }

    long checkpoint = Long.MAX_VALUE;
    try {
      for (final Future<Long> future : parallelExecutor.invokeAll(callables)) {
        long version = future.get();
        if (version < checkpoint) { // we must take the lowest checkpoint from all the slices
          checkpoint = version;
        }
      }
    }
    catch (ExecutionException | InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error while requesting slice's checkpoints", e);
    }
    finally {
      parallelExecutor.shutdown();
    }

    rsp.add("checkpoint", checkpoint);
  }

  /**
   * Retrieve the version number of the latest entry of the {@link org.apache.solr.update.UpdateLog}.
   */
  private void handleSliceCheckpointAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (!leaderStateManager.amILeader()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Action '" + CdcrAction.SLICECHECKPOINT +
          "' sent to non-leader replica");
    }

    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates();
    List<Long> versions = recentUpdates.getVersions(1);
    long lastVersion = versions.isEmpty() ? -1 : versions.get(0);
    rsp.add("checkpoint", lastVersion);
    recentUpdates.close();
  }

  private void handleEnableBufferAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (bufferStateManager.getState() == BufferState.DISABLED) {
      bufferStateManager.setState(BufferState.ENABLED);
      bufferStateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleDisableBufferAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (bufferStateManager.getState() == BufferState.ENABLED) {
      bufferStateManager.setState(BufferState.DISABLED);
      bufferStateManager.synchronize();
    }

    rsp.add(CdcrAction.STATUS.toLower(), this.getStatus());
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
    STATUS,
    COLLECTIONCHECKPOINT,
    SLICECHECKPOINT,
    ENABLEBUFFER,
    DISABLEBUFFER,
    TRIGGER;

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
   * The possible states of the CDCR process
   */
  public enum ProcessState {
    STARTED,
    STOPPED;

    public static ProcessState get(byte[] state) {
      if (state != null) {
        try {
          return ProcessState.valueOf(new String(state).toUpperCase(Locale.ROOT));
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

    public static String getParam() {
      return "process";
    }

  }

  /**
   * The possible states of the CDCR buffer
   */
  public enum BufferState {
    ENABLED,
    DISABLED;

    public static BufferState get(byte[] state) {
      if (state != null) {
        try {
          return BufferState.valueOf(new String(state).toUpperCase(Locale.ROOT));
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

    public static String getParam() {
      return "buffer";
    }

  }

  /**
   * A thread subclass for executing a single
   * {@link org.apache.solr.handler.CdcrRequestHandler.CdcrAction#SLICECHECKPOINT} action.
   */
  private static final class SliceCheckpointCallable implements Callable<Long> {

    final String baseUrl;
    final String cdcrPath;

    SliceCheckpointCallable(final String baseUrl, final String cdcrPath) {
      this.baseUrl = baseUrl;
      this.cdcrPath = cdcrPath;
    }

    @Override
    public Long call() throws Exception {
      HttpSolrServer server = new HttpSolrServer(baseUrl);
      try {
        server.setConnectionTimeout(15000);
        server.setSoTimeout(60000);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.ACTION, CdcrAction.SLICECHECKPOINT.toString());

        SolrRequest request = new QueryRequest(params);
        request.setPath(cdcrPath);

        NamedList response = server.request(request);
        return (Long) response.get("checkpoint");
      }
      finally {
        server.shutdown();
      }
    }

  }

}
