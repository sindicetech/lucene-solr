package org.apache.solr.update.processor;

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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;

/**
 *
 */
public class CdcrUpdateProcessor extends DistributedUpdateProcessor {
  public static final String CDCR_UPDATE = "cdcr.update";

  public CdcrUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  @Override
  protected boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    /*
    temporarily set the PEER_SYNC flag so that DistributedUpdateProcessor.versionAdd doesn't execute leader logic
    but the else part of that if. That way version remains preserved.
     */
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() | UpdateCommand.PEER_SYNC);
    }

    boolean result = super.versionAdd(cmd);

    // unset the flag to avoid unintended consequences down the chain
    if (cmd.getReq().getParams().get(CDCR_UPDATE) != null) {
      cmd.setFlags(cmd.getFlags() & ~UpdateCommand.PEER_SYNC);
    }

    return result;
  }
}
