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

package org.apache.solr.client.solrj.embedded;

import org.apache.solr.servlet.SolrDispatchFilter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.GzipHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.server.ssl.SslConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Run solr using jetty
 * 
 * @since solr 1.3
 */
public class JettySolrRunner {

  private static final Logger logger = LoggerFactory.getLogger(JettySolrRunner.class);

  Server server;

  FilterHolder dispatchFilter;
  FilterHolder debugFilter;

  private boolean waitOnSolr = false;
  private int lastPort = -1;

  private final JettyConfig config;
  private final String solrHome;
  private final Properties nodeProperties;
  
  private volatile boolean startedBefore = false;

  private LinkedList<FilterHolder> extraFilters;
  
  private int proxyPort = -1;

  public static class DebugFilter implements Filter {

    private AtomicLong nRequests = new AtomicLong();

    public long getTotalRequests() {
      return nRequests.get();

    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException { }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
      nRequests.incrementAndGet();
      filterChain.doFilter(servletRequest, servletResponse);
    }

    @Override
    public void destroy() { }

  }

  private static Properties defaultNodeProperties(String solrconfigFilename, String schemaFilename) {
    Properties props = new Properties();
    props.setProperty("solrconfig", solrconfigFilename);
    props.setProperty("schema", schemaFilename);
    return props;
  }

  /**
   * Create a new JettySolrRunner.
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solr home directory to use
   * @param context the context to run in
   * @param port the port to run on
   */
  public JettySolrRunner(String solrHome, String context, int port) {
    this(solrHome, JettyConfig.builder().setContext(context).setPort(port).build());
  }

  /**
   * @deprecated use {@link #JettySolrRunner(String,Properties,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName) {
    this(solrHome, defaultNodeProperties(solrConfigFilename, schemaFileName), JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .build());
  }

  /**
   * @deprecated use {@link #JettySolrRunner(String,Properties,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port,
      String solrConfigFilename, String schemaFileName, boolean stopAtShutdown) {
    this(solrHome, defaultNodeProperties(solrConfigFilename, schemaFileName),
        JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .build());
  }

  /**
   * Constructor taking an ordered list of additional (servlet holder -&gt; path spec) mappings
   * to add to the servlet context
   * @deprecated use {@link JettySolrRunner#JettySolrRunner(String,Properties,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port,
      String solrConfigFilename, String schemaFileName, boolean stopAtShutdown,
      SortedMap<ServletHolder,String> extraServlets) {
    this(solrHome, defaultNodeProperties(solrConfigFilename, schemaFileName),
        JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .build());
  }

  /**
   * @deprecated use {@link #JettySolrRunner(String,Properties,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName,
                         boolean stopAtShutdown, SortedMap<ServletHolder, String> extraServlets, SSLConfig sslConfig) {
    this(solrHome, defaultNodeProperties(solrConfigFilename, schemaFileName),
        JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .withSSLConfig(sslConfig)
        .build());
  }

  /**
   * @deprecated use {@link #JettySolrRunner(String,Properties,JettyConfig)}
   */
  @Deprecated
  public JettySolrRunner(String solrHome, String context, int port, String solrConfigFilename, String schemaFileName,
                         boolean stopAtShutdown, SortedMap<ServletHolder, String> extraServlets, SSLConfig sslConfig,
                         SortedMap<Class<? extends Filter>, String> extraRequestFilters) {
    this(solrHome, defaultNodeProperties(solrConfigFilename, schemaFileName),
        JettyConfig.builder()
        .setContext(context)
        .setPort(port)
        .stopAtShutdown(stopAtShutdown)
        .withServlets(extraServlets)
        .withFilters(extraRequestFilters)
        .withSSLConfig(sslConfig)
        .build());
  }

  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome    the base path to run from
   * @param config the configuration
   */
  public JettySolrRunner(String solrHome, JettyConfig config) {
    this(solrHome, new Properties(), config);
  }

  /**
   * Construct a JettySolrRunner
   *
   * After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome            the solrHome to use
   * @param nodeProperties      the container properties
   * @param config         the configuration
   */
  public JettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig config) {

    this.solrHome = solrHome;
    this.config = config;
    this.nodeProperties = nodeProperties;

    this.init(this.config.port);
  }

  private void init(int port) {

    server = new Server(port);
    server.setStopAtShutdown(config.stopAtShutdown);
    if (!config.stopAtShutdown) {
      server.setGracefulShutdown(0);
    }

    System.setProperty("solr.solr.home", solrHome);

    if (System.getProperty("jetty.testMode") != null) {
      final String connectorName = System.getProperty("tests.jettyConnector", "SelectChannel");

      // if this property is true, then jetty will be configured to use SSL
      // leveraging the same system properties as java to specify
      // the keystore/truststore if they are set unless specific config
      // is passed via the constructor.
      //
      // This means we will use the same truststore, keystore (and keys) for
      // the server as well as any client actions taken by this JVM in
      // talking to that server, but for the purposes of testing that should 
      // be good enough
      final SslContextFactory sslcontext = SSLConfig.createContextFactory(config.sslConfig);

      final Connector connector;
      if ("SelectChannel".equals(connectorName)) {
        final SelectChannelConnector c = sslcontext != null
          ? new SslSelectChannelConnector(sslcontext)
          : new SelectChannelConnector();
        c.setReuseAddress(true);
        c.setLowResourcesMaxIdleTime(1500);
        c.setSoLingerTime(0);
        connector = c;
      } else if ("Socket".equals(connectorName)) {
        final SocketConnector c = sslcontext != null
          ? new SslSocketConnector(sslcontext)
          : new SocketConnector();
        c.setReuseAddress(true);
        c.setSoLingerTime(0);
        connector = c;
      } else {
        throw new IllegalArgumentException("Illegal value for system property 'tests.jettyConnector': " + connectorName);
      }

      connector.setPort(port);
      connector.setHost("127.0.0.1");

      // Connectors by default inherit server's thread pool.
      QueuedThreadPool qtp = new QueuedThreadPool();
      qtp.setMaxThreads(10000);
      qtp.setMaxIdleTimeMs((int) TimeUnit.MILLISECONDS.toMillis(200));
      qtp.setMaxStopTimeMs((int) TimeUnit.MINUTES.toMillis(1));
      server.setThreadPool(qtp);

      server.setConnectors(new Connector[] {connector});
      server.setSessionIdManager(new HashSessionIdManager(new Random()));
    } else {
      if (server.getThreadPool() == null) {
        // Connectors by default inherit server's thread pool.
        QueuedThreadPool qtp = new QueuedThreadPool();
        qtp.setMaxThreads(10000);
        qtp.setMaxIdleTimeMs((int) TimeUnit.SECONDS.toMillis(5));
        qtp.setMaxStopTimeMs((int) TimeUnit.SECONDS.toMillis(1));
        server.setThreadPool(qtp);
      }
    }

    // Initialize the servlets
    final ServletContextHandler root = new ServletContextHandler(server, config.context, ServletContextHandler.SESSIONS);
    root.setHandler(new GzipHandler());
    server.addLifeCycleListener(new LifeCycle.Listener() {

      @Override
      public void lifeCycleStopping(LifeCycle arg0) {
      }

      @Override
      public void lifeCycleStopped(LifeCycle arg0) {}

      @Override
      public void lifeCycleStarting(LifeCycle arg0) {
        synchronized (JettySolrRunner.this) {
          waitOnSolr = true;
          JettySolrRunner.this.notify();
        }
      }

      @Override
      public void lifeCycleStarted(LifeCycle arg0) {

        lastPort = getFirstConnectorPort();
        nodeProperties.setProperty("hostPort", Integer.toString(lastPort));
        nodeProperties.setProperty("hostContext", config.context);

        root.getServletContext().setAttribute(SolrDispatchFilter.PROPERTIES_ATTRIBUTE, nodeProperties);
        root.getServletContext().setAttribute(SolrDispatchFilter.SOLRHOME_ATTRIBUTE, solrHome);

        logger.info("Jetty properties: {}", nodeProperties);

        debugFilter = root.addFilter(DebugFilter.class, "*", EnumSet.of(DispatcherType.REQUEST) );
        extraFilters = new LinkedList<>();
        for (Class<? extends Filter> filterClass : config.extraFilters.keySet()) {
          extraFilters.add(root.addFilter(filterClass, config.extraFilters.get(filterClass),
              EnumSet.of(DispatcherType.REQUEST)));
        }

        dispatchFilter = root.addFilter(SolrDispatchFilter.class, "*", EnumSet.of(DispatcherType.REQUEST) );
        for (ServletHolder servletHolder : config.extraServlets.keySet()) {
          String pathSpec = config.extraServlets.get(servletHolder);
          root.addServlet(servletHolder, pathSpec);
        }

      }

      @Override
      public void lifeCycleFailure(LifeCycle arg0, Throwable arg1) {
        System.clearProperty("hostPort");
      }
    });

    // for some reason, there must be a servlet for this to get applied
    root.addServlet(Servlet404.class, "/*");

  }

  public FilterHolder getDispatchFilter() {
    return dispatchFilter;
  }

  public boolean isRunning() {
    return server.isRunning();
  }
  
  public boolean isStopped() {
    return server.isStopped();
  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  /**
   * Start the Jetty server
   *
   * @throws Exception if an error occurs on startup
   */
  public void start() throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    try {
      // if started before, make a new server
      if (startedBefore) {
        waitOnSolr = false;
        init(lastPort);
      } else {
        startedBefore = true;
      }

      if (!server.isRunning()) {
        server.start();
      }
      synchronized (JettySolrRunner.this) {
        int cnt = 0;
        while (!waitOnSolr) {
          this.wait(100);
          if (cnt++ == 5) {
            throw new RuntimeException("Jetty/Solr unresponsive");
          }
        }
      }
    } finally {
      if (prevContext != null)  {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Stop the Jetty server
   *
   * @throws Exception if an error occurs on shutdown
   */
  public void stop() throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    try {
      Filter filter = dispatchFilter.getFilter();

      server.stop();

      if (server.getState().equals(Server.FAILED)) {
        filter.destroy();
        if (extraFilters != null) {
          for (FilterHolder f : extraFilters) {
            f.getFilter().destroy();
          }
        }
      }

      server.join();
    } finally {
      if (prevContext != null)  {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Returns the Local Port of the jetty Server.
   * 
   * @exception RuntimeException if there is no Connector
   */
  private int getFirstConnectorPort() {
    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new RuntimeException("Jetty Server has no Connectors");
    }
    return (proxyPort != -1) ? proxyPort : conns[0].getLocalPort();
  }
  
  /**
   * Returns the Local Port of the jetty Server.
   * 
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    if (lastPort == -1) {
      throw new IllegalStateException("You cannot get the port until this instance has started");
    }
    return (proxyPort != -1) ? proxyPort : lastPort;
  }
  
  /**
   * Sets the port of a local socket proxy that sits infront of this server; if set
   * then all client traffic will flow through the proxy, giving us the ability to
   * simulate network partitions very easily.
   */
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * Returns a base URL consisting of the protocal, host, and port for a 
   * Connector in use by the Jetty Server contained in this runner.
   */
  public URL getBaseUrl() {
    String protocol = null;
    try {
      Connector[] conns = server.getConnectors();
      if (0 == conns.length) {
        throw new IllegalStateException("Jetty Server has no Connectors");
      }
      Connector c = conns[0];
      if (c.getLocalPort() < 0) {
        throw new IllegalStateException("Jetty Connector is not open: " + 
                                        c.getLocalPort());
      }
      protocol = (c instanceof SslConnector) ? "https" : "http";
      return new URL(protocol, c.getHost(), c.getLocalPort(), config.context);

    } catch (MalformedURLException e) {
      throw new  IllegalStateException
        ("Java could not make sense of protocol: " + protocol, e);
    }
  }

  public DebugFilter getDebugFilter() {
    return (DebugFilter)debugFilter.getFilter();
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /**
   * This is a stupid hack to give jetty something to attach to
   */
  public static class Servlet404 extends HttpServlet {
    @Override
    public void service(HttpServletRequest req, HttpServletResponse res)
        throws IOException {
      res.sendError(404, "Can not find: " + req.getRequestURI());
    }
  }

  /**
   * A main class that starts jetty+solr This is useful for debugging
   */
  public static void main(String[] args) {
    try {
      JettySolrRunner jetty = new JettySolrRunner(".", "/solr", 8983);
      jetty.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * @deprecated set properties in the Properties passed to the constructor
   */
  @Deprecated
  public void setShards(String shardList) {
     nodeProperties.setProperty("shard", shardList);
  }

  /**
   * @deprecated set properties in the Properties passed to the constructor
   */
  @Deprecated
  public void setDataDir(String dataDir) {
    nodeProperties.setProperty("solr.data.dir", dataDir);
  }

  /**
   * @deprecated set properties in the Properties passed to the constructor
   */
  @Deprecated
  public void setUlogDir(String ulogDir) {
    nodeProperties.setProperty("solr.ulog.dir", ulogDir);
  }

  /**
   * @deprecated set properties in the Properties passed to the constructor
   */
  @Deprecated
  public void setCoreNodeName(String coreNodeName) {
    nodeProperties.setProperty("coreNodeName", coreNodeName);
  }

  /**
   * @return the Solr home directory of this JettySolrRunner
   */
  public String getSolrHome() {
    return solrHome;
  }
}
