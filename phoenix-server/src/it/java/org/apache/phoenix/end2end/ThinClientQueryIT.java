/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;

/**
 * Runs {@link QueryIT} but through the query server.
 */
public class ThinClientQueryIT extends QueryIT {

  private static QueryServerThread t;

  public ThinClientQueryIT(String indexDDL) {
    super(indexDDL);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    // ensure the mini-cluster is running, get url
    String thickUrl = getUrl();
    // launch a query server vs. that cluster
    t = new QueryServerThread(new String[] { url }, getTestClusterConfig(),
        ThinClientQueryIT.class.getName());
    t.start();
    url = ThinClientUtil.getConnectionUrl("localhost", t.getMain().getPort());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    t.join(5000);
    assertEquals("query server didn't exit cleanly", 0, t.getMain().getRetCode());
  }
}
