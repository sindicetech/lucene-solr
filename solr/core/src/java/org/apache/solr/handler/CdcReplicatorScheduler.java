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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CdcReplicatorScheduler {

  private boolean isStarted = false;

  private ScheduledExecutorService scheduler;
  private ExecutorService replicatorsPool;

  private final CdcReplicatorManager replicatorManager;
  private final ConcurrentLinkedQueue<CdcReplicatorState> statesQueue;

  public static final int POOL_SIZE = 8;

  protected static Logger log = LoggerFactory.getLogger(CdcReplicatorScheduler.class);

  CdcReplicatorScheduler(final CdcReplicatorManager replicatorStatesManager) {
    this.replicatorManager = replicatorStatesManager;
    this.statesQueue = new ConcurrentLinkedQueue<>(replicatorManager.getReplicatorStates());
  }

  void start() {
    if (!isStarted) {
      scheduler = Executors.newSingleThreadScheduledExecutor();
      replicatorsPool = Executors.newFixedThreadPool(POOL_SIZE);

      // the scheduler thread is executed every second and submits one replication task
      // per available state in the queue
      scheduler.scheduleWithFixedDelay(new Runnable() {

        @Override
        public void run() {
          int nCandidates = statesQueue.size();
          for (int i = 0; i < nCandidates; i++) {
            // a thread that pool one state from the queue, execute the replication task, and push back
            // the state in the queue when the task is completed
            replicatorsPool.execute(new Runnable() {

              @Override
              public void run() {
                CdcReplicatorState state = statesQueue.poll();
                try {
                  new CdcReplicator(state).run();
                }
                finally {
                  statesQueue.offer(state);
                }
              }

            });

          }
        }

      }, 0, 1, TimeUnit.SECONDS);
      isStarted = true;
    }
  }

  void shutdown() {
    if (isStarted) {
      replicatorsPool.shutdown();
      try {
        replicatorsPool.awaitTermination(60, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
        log.warn("Thread interrupted while waiting for CDCR replicator threadpool close.");
        Thread.currentThread().interrupt();
      }
      finally {
        scheduler.shutdownNow();
        isStarted = false;
      }
    }
  }

}
