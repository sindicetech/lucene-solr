/**
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

package org.apache.solr.handler;

import java.util.Locale;

public class CdcrParams {

  /** The definition of a replica configuration **/
  public static final String REPLICA_PARAM = "replica";

  /** The source collection of a replica **/
  public static final String SOURCE_COLLECTION_PARAM = "source";

  /** The target collection of a replica **/
  public static final String TARGET_COLLECTION_PARAM = "target";

  /** The Zookeeper host of the target cluster hosting the replica **/
  public static final String ZK_HOST_PARAM = "zkHost";

  /** The definition of the {@link org.apache.solr.handler.CdcReplicatorScheduler} configuration **/
  public static final String REPLICATOR_PARAM = "replicator";

  /** The thread pool size of the replicator **/
  public static final String THREAD_POOL_SIZE_PARAM = "threadPoolSize";

  /** The time schedule (in ms) of the replicator **/
  public static final String SCHEDULE_PARAM = "schedule";

  /** The definition of the {@link org.apache.solr.handler.CdcrUpdateLogSynchronizer} configuration **/
  public static final String UPDATE_LOG_SYNCHRONIZER_PARAM = "updateLogSynchronizer";

  /** The latest update checkpoint on a target cluster **/
  public final static String CHECKPOINT = "checkpoint";

  /** The last processed version on a source cluster **/
  public final static String LAST_PROCESSED_VERSION = "lastProcessedVersion";

  /** A list of replica queues on a source cluster **/
  public final static String QUEUES = "queues";

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
    LASTPROCESSEDVERSION,
    QUEUESIZE;

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
}
