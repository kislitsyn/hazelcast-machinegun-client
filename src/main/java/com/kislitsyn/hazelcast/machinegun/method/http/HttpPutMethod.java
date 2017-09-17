package com.kislitsyn.hazelcast.machinegun.method.http;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.impl.ClientLoggingService;
import com.hazelcast.client.spi.impl.DefaultAddressProvider;
import com.hazelcast.client.spi.impl.discovery.DiscoveryAddressProvider;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.properties.GroupProperty;
import com.kislitsyn.hazelcast.machinegun.method.PutMethod;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Http method of data insertion. Use Hazelcast Service discover for configure client
 *
 * Created by Anton Kislitsyn on 10/09/2017
 */
public class HttpPutMethod implements PutMethod {

    private static final Logger log = LoggerFactory.getLogger(HttpPutMethod.class);

    private final BiConsumer<String, String> consumer;

    public HttpPutMethod(String mapName) {
        ClientConfig config = new XmlClientConfigBuilder().build();

        InetSocketAddress inetSocketAddress = getAddress(config);

        HttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        CloseableHttpClient client = HttpClientBuilder.create().setConnectionManager(connectionManager).build();

        consumer = (key, value) -> postMapValue(inetSocketAddress, client, mapName, key, value);
    }

    private void postMapValue(InetSocketAddress address,
                              CloseableHttpClient client,
                              String mapName,
                              String key,
                              String value) {
        URI uri;
        try {
            uri = new URIBuilder()
                    .setScheme(HttpHost.DEFAULT_SCHEME_NAME)
                    .setHost(address.getHostString())
                    .setPort(address.getPort())
                    .setPath(String.format("hazelcast/rest/maps/%s/%s", mapName, key))
                    .build();
        } catch (URISyntaxException e) {
            log.error("uri create failed");
            throw new IllegalStateException(e);
        }
        CloseableHttpResponse response = null;
        try {
            HttpEntity entity = new StringEntity(value);
            HttpUriRequest request = RequestBuilder.post(uri)
                    .addHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType())
                    .setEntity(entity)
                    .build();
            response = client.execute(request);
        } catch (Exception e) {
            log.error("put request failed", e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error("response close error", e);
                }
            }
        }
    }

    private InetSocketAddress getAddress(ClientConfig config) {
        String instanceName;
        if (config.getInstanceName() != null) {
            instanceName = config.getInstanceName();
        } else {
            instanceName = "hz.client_" + UUID.randomUUID().toString();
        }

        GroupConfig groupConfig = config.getGroupConfig();
        String loggingType = config.getProperty(GroupProperty.LOGGING_TYPE.getName());
        ClientLoggingService loggingService = new ClientLoggingService(groupConfig.getName(),
                loggingType, BuildInfoProvider.getBuildInfo(), instanceName);

        DiscoveryService discoveryService = initDiscoveryService(config, loggingService);

        AddressProvider addressProvider;
        if (discoveryService == null) {
            if (config.getNetworkConfig().getAddresses() == null) {
                throw new IllegalStateException("addresses is absent");
            }

            addressProvider = new DefaultAddressProvider(config.getNetworkConfig());
        } else {
            addressProvider = new DiscoveryAddressProvider(discoveryService, loggingService);
        }

        Collection<InetSocketAddress> inetSocketAddresses = addressProvider.loadAddresses();
        log.info("discovered addresses: {}", inetSocketAddresses);

        if (inetSocketAddresses == null || inetSocketAddresses.size() == 0) {
            throw new IllegalStateException("can't find cluster");
        }

        return inetSocketAddresses.iterator().next();
    }

    private DiscoveryService initDiscoveryService(ClientConfig config, ClientLoggingService loggingService) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        if (!Boolean.TRUE.equals(Boolean.valueOf((String) config.getProperties().get(ClientProperty.DISCOVERY_SPI_ENABLED.getName())))) {
            log.info("discovery settings is disabled: {}={}", ClientProperty.DISCOVERY_SPI_ENABLED,
                    config.getProperties().get(ClientProperty.DISCOVERY_SPI_ENABLED.getName()));
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = networkConfig.getDiscoveryConfig().getAsReadOnly();

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings().setConfigClassLoader(config.getClassLoader())
                .setLogger(logger).setDiscoveryMode(DiscoveryMode.Client).setDiscoveryConfig(discoveryConfig);

        DiscoveryService discoveryService = factory.newDiscoveryService(settings);
        discoveryService.start();
        return discoveryService;
    }

    @Override
    public String getName() {
        return "http";
    }

    @Override
    public BiConsumer<String, String> getConsumer() {
        return consumer;
    }
}
