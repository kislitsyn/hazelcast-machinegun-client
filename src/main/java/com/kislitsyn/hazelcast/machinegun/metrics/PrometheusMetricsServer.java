package com.kislitsyn.hazelcast.machinegun.metrics;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Created by Anton Kislitsyn on 19/08/2017
 */
public class PrometheusMetricsServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsServer.class);

    private final HTTPServer server;

    public PrometheusMetricsServer(int port) throws IOException {
        DefaultExports.initialize();
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        server = new HTTPServer(hostAddress, port);
        log.info("http server started: {}:{}", hostAddress, port);
    }

    @Override
    public void close() throws IOException {
        server.stop();
        log.info("server stopped");
    }
}
