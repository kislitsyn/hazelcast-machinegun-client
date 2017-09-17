package com.kislitsyn.hazelcast.machinegun.metrics;

import io.prometheus.client.Histogram;

/**
 * Metrics container
 *
 * Created by Anton Kislitsyn on 25/08/2017
 */
public class PrometheusMetrics {

    private final Histogram putLatency = Histogram.build("put_latency", "Put new entry latency")
            .subsystem("map")
            .namespace("hazelcast")
            .labelNames("name", "method")
            .register();

    public PrometheusMetrics() {
    }

    public Histogram getPutLatency() {
        return putLatency;
    }
}
