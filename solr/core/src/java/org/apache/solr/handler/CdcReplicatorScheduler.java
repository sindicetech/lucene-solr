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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcReplicatorScheduler {

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private final ExecutorService replicatorsPool = Executors.newFixedThreadPool(POOL_SIZE);

  private ScheduledFuture<?> schedulerHandle;

  private final CdcReplicatorStatesManager replicatorStatesManager;

  private CdcrRequestHandler.ProcessState processState = CdcrProcessStateManager.DEFAULT_STATE;
  private boolean amILeader = false;

  public static final int POOL_SIZE = 8;

  protected static Logger log = LoggerFactory.getLogger(CdcReplicatorScheduler.class);

  CdcReplicatorScheduler(final CdcReplicatorStatesManager replicatorStatesManager) {
    this.replicatorStatesManager = replicatorStatesManager;
  }

  /**
   * If we become the leader and the process state is STARTED, we need to initialise the log readers.
   * Otherwise, if the process state is STOPPED pr if we are not the leader, we need to close the log readers.
   */
  void inform(boolean amILeader) {
    this.amILeader = amILeader;
    if (amILeader && processState.equals(CdcrRequestHandler.ProcessState.STARTED)) {
      this.start();
      return;
    }
    this.stop();
  }

  /**
   * If the process state changes to STARTED, and we are the leader, we need to initialise the log readers. Otherwise,
   * we need to close the log readers.
   */
  void inform(CdcrRequestHandler.ProcessState state) {
    this.processState = state;
    if (amILeader && processState.equals(CdcrRequestHandler.ProcessState.STARTED)) {
      this.start();
      return;
    }
    this.stop();
  }

  void start() {
    schedulerHandle = scheduler.scheduleWithFixedDelay(new Runnable() {

      @Override
      public void run() {
        for (CdcReplicatorState state : replicatorStatesManager.getReplicatorStates()) {
          replicatorsPool.submit(new CdcReplicator(state));
        }
      }

    }, 1, 1, TimeUnit.SECONDS);
  }

  void stop() {
    if (schedulerHandle != null) {
      schedulerHandle.cancel(false);
    }
  }

}
