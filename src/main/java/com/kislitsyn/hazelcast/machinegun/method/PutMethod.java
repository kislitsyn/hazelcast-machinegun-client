package com.kislitsyn.hazelcast.machinegun.method;

import java.util.function.BiConsumer;

/**
 * Created by Anton Kislitsyn on 31/08/2017
 */
public interface PutMethod {

    String getName();

    BiConsumer<String, String> getConsumer();
}
