package com.kislitsyn.hazelcast.machinegun;

import com.kislitsyn.hazelcast.machinegun.method.PutMethod;
import com.kislitsyn.hazelcast.machinegun.method.PutMethodFactory;
import com.kislitsyn.hazelcast.machinegun.metrics.PrometheusMetrics;
import com.kislitsyn.hazelcast.machinegun.metrics.PrometheusMetricsServer;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Created by Anton Kislitsyn on 03/12/2016
 */
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final String METRICS_PORT = "metrics.port";

    public static void main(String[] args) throws IOException {
        Properties props = System.getProperties();
        Map<String, String> properties = props.stringPropertyNames().stream()
                .collect(Collectors.toMap(name -> name, props::getProperty));

        log.info("logging level: {}", properties.get("logging.threshold"));
        Configurator.setRootLevel(Level.toLevel(properties.get("logging.threshold"), Level.TRACE));

        PrometheusMetricsServer metricsServer =
                new PrometheusMetricsServer(Integer.valueOf(properties.getOrDefault(METRICS_PORT, "8080")));

        String mapName = properties.getOrDefault("hazelcast.client.mapName", "default");

        PrometheusMetrics monitor = new PrometheusMetrics();

        PutMethod method = new PutMethodFactory().createMethod(mapName, properties);

        RandomStringGenerator stringGenerator = new RandomStringGenerator.Builder().build();
        Runnable runnable = () -> insertMapEntry(method.getConsumer(), stringGenerator);

        Runnable command = () -> {
            try {
                monitor.getPutLatency().labels(mapName, method.getName()).time(runnable);
            } catch (Exception e) {
                log.error("histogram time observation failed", e);
            }
        };

        String corePoolSizeStr = properties.getOrDefault("hazelcast.client.corePoolSize", "100");
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(Integer.valueOf(corePoolSizeStr));

        Integer delay = Integer.valueOf(properties.getOrDefault("hazelcast.client.delay", "10"));
        executor.scheduleWithFixedDelay(
                command,
                0,
                delay,
                TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                metricsServer.close();
            } catch (IOException e) {
                log.error("shutdown failed", e);
            }
        }));

        log.info("started: corePoolSize={}, delay={}ms", corePoolSizeStr, delay);
    }

    private static void insertMapEntry(BiConsumer<String, String> consumer, RandomStringGenerator stringGenerator) {
        try {
            String key = UUID.randomUUID().toString();
            String value = stringGenerator.generate(5000);
            consumer.accept(key, value);
            log.debug("put entry: key={}, value={}", key, value.substring(0, 10));
        } catch (Exception e) {
            log.error("put failed", e);
        }
    }
}
