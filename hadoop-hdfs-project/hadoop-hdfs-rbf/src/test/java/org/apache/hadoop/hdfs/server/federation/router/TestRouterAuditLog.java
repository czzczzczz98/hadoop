/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.federation.router;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

public class TestRouterAuditLog {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterRpc.class);

  private static LogCapturer auditlog;

  /** Federated HDFS cluster. */
  private static MiniRouterDFSCluster cluster;

  /** Random Router for this federated cluster. */
  private MiniRouterDFSCluster.RouterContext router;

  @Before
  public void testSetup() throws Exception {
    Configuration namenodeConf = new Configuration();
    namenodeConf.setBoolean(DFSConfigKeys.HADOOP_CALLER_CONTEXT_ENABLED_KEY,
        true);
    cluster = new MiniRouterDFSCluster(false, 2);
    // We need 6 DNs to test Erasure Coding with RS-6-3-64k
    cluster.setNumDatanodesPerNameservice(6);
    cluster.addNamenodeOverrides(namenodeConf);

    // Start NNs and DNs and wait until ready
    cluster.startCluster();

    // Start routers with only an RPC service
    Configuration routerConf = new RouterConfigBuilder()
        .metrics()
        .rpc()
        .build();
    // We decrease the DN cache times to make the test faster
    routerConf.setTimeDuration(
        RBFConfigKeys.DN_REPORT_CACHE_EXPIRE, 1, TimeUnit.SECONDS);
    cluster.addRouterOverrides(routerConf);
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    auditlog = LogCapturer.captureLogs(RouterRpcServer.auditLog);
    // Create mock locations
    cluster.installMockLocations();

    // Delete all files via the NNs and verify
    cluster.deleteAllFiles();

    // Create test fixtures on NN
    cluster.createTestDirectoriesNamenode();

    // Wait to ensure NN has fully created its test directories
    Thread.sleep(100);

    // Random router for this test
    MiniRouterDFSCluster.RouterContext rndRouter = cluster.getRandomRouter();
    this.router = rndRouter;
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testGetListing() throws IOException {
    DFSClient client = null;
    try {
      client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.getListing("/", HdfsFileStatus.EMPTY_NAME, false);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String acePattern = ".*cmd=getListing.*";
    verifyAuditLogs(acePattern);
  }

  @Test
  public void testGetFileInfo() throws IOException {
    DFSClient client = null;
    try {
      client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.getFileInfo("/");
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String acePattern = ".*cmd=getFileInfo.*";
    verifyAuditLogs(acePattern);
  }

  @Test
  public void testGetServerDefaults() throws IOException {
    DFSClient client = null;
    try {
      client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.getServerDefaults();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    String acePattern = ".*cmd=getServerDefaults.*";
    verifyAuditLogs(acePattern);
  }

  private void verifyAuditLogs(String pattern) {
    int length = auditlog.getOutput().split(System.lineSeparator()).length;
    String lastAudit = auditlog.getOutput()
        .split(System.lineSeparator())[length - 1];
    assertTrue("Unexpected log!", lastAudit.matches(pattern));
  }


}