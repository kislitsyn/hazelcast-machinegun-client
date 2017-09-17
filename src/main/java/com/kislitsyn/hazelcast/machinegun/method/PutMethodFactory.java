package com.kislitsyn.hazelcast.machinegun.method;

import com.kislitsyn.hazelcast.machinegun.method.http.HttpPutMethod;
import com.kislitsyn.hazelcast.machinegun.method.socket.SocketPutMethod;

import java.util.Map;


/**
 * Factory of methods for data insertion
 *
 * Created by Anton Kislitsyn on 31/08/2017
 */
public class PutMethodFactory {

    public PutMethod createMethod(String mapName, Map<String, String> properties) {
        String method = properties.get("hazelcast.client.method");
        if ("http".equals(method)) {
            return new HttpPutMethod(mapName);
        }
        return new SocketPutMethod(mapName);
    }
}
