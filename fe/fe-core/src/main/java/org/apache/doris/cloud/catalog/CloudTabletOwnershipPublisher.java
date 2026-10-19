// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPublishTabletOwnershipRequest;
import org.apache.doris.thrift.TPublishTabletOwnershipResponse;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tells every backend which of the tablets it may have cached it owns: those whose primary
 * backend in its compute group it is. A backend keeps the tablets it owns, and drops a tablet it
 * does not own once nothing has used it for a while, such as a tablet it served while its owner
 * was restarting. Each publish sends the full set, so a lost one costs nothing but its interval.
 */
public class CloudTabletOwnershipPublisher extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudTabletOwnershipPublisher.class);

    public CloudTabletOwnershipPublisher() {
        super("cloud-tablet-ownership-publisher", intervalMs());
    }

    private static long intervalMs() {
        return Math.max(1L, Config.cloud_tablet_ownership_publish_interval_second) * 1000L;
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(intervalMs());
        if (!Config.isCloudMode() || !Config.enable_cloud_tablet_ownership_publish) {
            return;
        }
        Map<String, List<Backend>> clusterToBackends =
                ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdToBackend(false);
        Map<Long, String> backendToCluster = new HashMap<>();
        Map<Long, Backend> backends = new HashMap<>();
        for (Map.Entry<String, List<Backend>> entry : clusterToBackends.entrySet()) {
            for (Backend be : entry.getValue()) {
                backendToCluster.put(be.getId(), entry.getKey());
                backends.put(be.getId(), be);
            }
        }
        long generatedAtMs = System.currentTimeMillis();
        Map<Long, List<Long>> owned = collectOwnedTablets(clusterToBackends.keySet(), backendToCluster);
        int published = 0;
        for (Backend be : backends.values()) {
            if (!be.isAlive()) {
                continue;
            }
            List<Long> tablets = owned.getOrDefault(be.getId(), new ArrayList<>());
            if (publish(be, backendToCluster.get(be.getId()), generatedAtMs, tablets)) {
                published++;
            }
        }
        LOG.info("tablet ownership: published to {} of {} backends in {} ms", published, backends.size(),
                System.currentTimeMillis() - generatedAtMs);
    }

    private Map<Long, List<Long>> collectOwnedTablets(Collection<String> clusterIds,
            Map<Long, String> backendToCluster) {
        Map<Long, List<Long>> owned = new HashMap<>();
        for (Long dbId : Env.getCurrentInternalCatalog().getDbIds()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            for (Table table : db.getTables()) {
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        // Shadow indexes of a running schema change are owned as well.
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                            for (Tablet tablet : index.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica instanceof CloudReplica) {
                                        addOwners(owned, tablet.getId(), (CloudReplica) replica, clusterIds,
                                                backendToCluster);
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        return owned;
    }

    /**
     * Adds the tablet to the owned set of its primary backend in each compute group. A backend
     * that serves it only while that primary is down does not own it.
     */
    @VisibleForTesting
    static void addOwners(Map<Long, List<Long>> owned, long tabletId, CloudReplica replica,
            Collection<String> clusterIds, Map<Long, String> backendToCluster) {
        for (String clusterId : clusterIds) {
            long beId = replica.getClusterPrimaryBackendId(clusterId);
            if (beId > 0 && clusterId.equals(backendToCluster.get(beId))) {
                owned.computeIfAbsent(beId, k -> new ArrayList<>()).add(tabletId);
            }
        }
    }

    private boolean publish(Backend be, String clusterId, long generatedAtMs, List<Long> tablets) {
        TPublishTabletOwnershipRequest request = new TPublishTabletOwnershipRequest();
        request.setClusterId(clusterId);
        request.setGeneratedAtMs(generatedAtMs);
        request.setOwnedTabletIds(tablets);
        TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBePort());
        BackendService.Client client = null;
        boolean ok = false;
        try {
            client = ClientPool.backendPool.borrowObject(address);
            TPublishTabletOwnershipResponse response = client.publishTabletOwnership(request);
            ok = true;
            if (response.isSetStatus() && response.getStatus().getStatusCode() != TStatusCode.OK) {
                LOG.warn("tablet ownership: backend {} refused {} tablets: {}", be.getId(), tablets.size(),
                        response.getStatus().getErrorMsgs());
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.warn("tablet ownership: failed to publish {} tablets to backend {}: {}", tablets.size(),
                    be.getId(), e.getMessage());
            return false;
        } finally {
            if (client != null) {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
    }
}
