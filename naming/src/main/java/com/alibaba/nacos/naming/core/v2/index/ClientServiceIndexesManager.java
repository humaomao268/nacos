/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core.v2.index;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.event.service.ServiceEvent;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client and service index manager.
 *
 * @author xiweng.yy
 */
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {
    // 服务的所有发布者列表
    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();
    // 服务的所有订阅者列表
    private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();
    
    public ClientServiceIndexesManager() {
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    // 得到某个服务的所有注册客户端
    public Collection<String> getAllClientsRegisteredService(Service service) {
        return publisherIndexes.containsKey(service) ? publisherIndexes.get(service) : new ConcurrentHashSet<>();
    }
    // 得到某个服务的所有订阅者客户端
    public Collection<String> getAllClientsSubscribeService(Service service) {
        return subscriberIndexes.containsKey(service) ? subscriberIndexes.get(service) : new ConcurrentHashSet<>();
    }
    // 所有订阅的服务
    public Collection<Service> getSubscribedService() {
        return subscriberIndexes.keySet();
    }
    
    /**
     * Clear the service index without instances.
     *
     * @param service The service of the Nacos.
     */
    public void removePublisherIndexesByEmptyService(Service service) {
        if (publisherIndexes.containsKey(service) && publisherIndexes.get(service).isEmpty()) {
            publisherIndexes.remove(service);
        }
    }

    /**
     *
     * <p>该订阅器订阅那些类型的事件.</p>
     *
     * @return
     */
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);
        result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);
        result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);
        result.add(ClientEvent.ClientDisconnectEvent.class);
        return result;
    }

    /**
     *
     * <p>事件分2类.</p>
     * <p>上线、下线、订阅、取消订阅 属于ClientOperationEvent.</p>
     *
     * @param event {@link Event}
     */
    @Override
    public void onEvent(Event event) {
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            handleClientDisconnect((ClientEvent.ClientDisconnectEvent) event);
        } else if (event instanceof ClientOperationEvent) {
            handleClientOperation((ClientOperationEvent) event);
        }
    }

    /**
     *
     * <p>客户端下线事件的处理.</p>
     *
     * @param event {@link Event}
     */
    private void handleClientDisconnect(ClientEvent.ClientDisconnectEvent event) {
        Client client = event.getClient();
        for (Service each : client.getAllSubscribeService()) {
            removeSubscriberIndexes(each, client.getClientId());
        }
        DeregisterInstanceReason reason = event.isNative()
                ? DeregisterInstanceReason.NATIVE_DISCONNECTED : DeregisterInstanceReason.SYNCED_DISCONNECTED;
        long currentTimeMillis = System.currentTimeMillis();
        for (Service each : client.getAllPublishedService()) {
            removePublisherIndexes(each, client.getClientId());
            InstancePublishInfo instance = client.getInstancePublishInfo(each);
            NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(currentTimeMillis,
                    "", false, reason, each.getNamespace(), each.getGroup(), each.getName(),
                    instance.getIp(), instance.getPort()));
        }
    }
    
    private void handleClientOperation(ClientOperationEvent event) {
        Service service = event.getService();
        String clientId = event.getClientId();
        if (event instanceof ClientOperationEvent.ClientRegisterServiceEvent) {
            // 服务注册
            addPublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientDeregisterServiceEvent) {
            // 服务下线
            removePublisherIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientSubscribeServiceEvent) {
            // 服务订阅
            addSubscriberIndexes(service, clientId);
        } else if (event instanceof ClientOperationEvent.ClientUnsubscribeServiceEvent) {
            // 取消订阅
            removeSubscriberIndexes(service, clientId);
        }
    }
    
    private void addPublisherIndexes(Service service, String clientId) {
        publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        publisherIndexes.get(service).add(clientId);
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
    }

    /**
     *
     * <p>从发布服务列表中移除该服务的该clientId.</p>
     *
     * @param service 服务
     * @param clientId 客户端id
     */
    private void removePublisherIndexes(Service service, String clientId) {
        publisherIndexes.computeIfPresent(service, (s, ids) -> {
            ids.remove(clientId);
            NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, true));
            return ids.isEmpty() ? null : ids;
        });
    }
    
    private void addSubscriberIndexes(Service service, String clientId) {
        subscriberIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
        // Fix #5404, Only first time add need notify event.
        if (subscriberIndexes.get(service).add(clientId)) {
            NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
        }
    }

    /**
     *
     * <p>从该服务的订阅者列表中移除该clientId.</p>
     *
     * @param service 服务
     * @param clientId 客户端id
     */
    private void removeSubscriberIndexes(Service service, String clientId) {
        if (!subscriberIndexes.containsKey(service)) {
            return;
        }
        // 某个服务没有订阅者是key也一并删除掉
        subscriberIndexes.get(service).remove(clientId);
        if (subscriberIndexes.get(service).isEmpty()) {
            subscriberIndexes.remove(service);
        }
    }
}
