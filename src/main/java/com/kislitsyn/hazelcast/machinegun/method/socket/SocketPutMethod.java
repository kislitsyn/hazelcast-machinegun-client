package com.kislitsyn.hazelcast.machinegun.method.socket;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kislitsyn.hazelcast.machinegun.method.PutMethod;

import java.util.function.BiConsumer;

/**
 * Default data insertion method
 *
 * Created by Anton Kislitsyn on 10/09/2017
 */
public class SocketPutMethod implements PutMethod {

    private final IMap<String, String> map;

    public SocketPutMethod(String mapName) {
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        map = client.getMap(mapName);
    }

    @Override
    public String getName() {
        return "socket";
    }

    @Override
    public BiConsumer<String, String> getConsumer() {
        return map::put;
    }
}
