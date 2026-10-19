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
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.CompactionTask;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Finds tablets that cumulative compaction left in many small segments after loads into them
 * stopped, and runs a full compaction on each, so that a closed partition ends up in about as
 * many segments as its size calls for.
 *
 * <p>A tablet qualifies when its segment count is at least
 * {@link Config#final_compaction_segment_ratio} times the count its size needs at the table's
 * output segment size, and no new version became visible in its partition for
 * {@link Config#final_compaction_quiet_second}. The full compactions run on the tablet's backend
 * in {@link Config#final_compaction_compute_group}, at most
 * {@link Config#final_compaction_max_tasks_per_be} at a time per backend. Each backend reports
 * the end of the task, after which the tablet waits {@link Config#final_compaction_cooldown_second}
 * before it is considered again.
 *
 * <p>All state is in memory on the master.
 */
public class FinalCompactionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(FinalCompactionScheduler.class);

    private static final int VERSION_BATCH_SIZE = 500;

    /** A full compaction dispatched by this scheduler whose end has not been reported yet. */
    @VisibleForTesting
    static final class InFlight {
        final long backendId;
        final long dispatchTimeMs;

        InFlight(long backendId, long dispatchTimeMs) {
            this.backendId = backendId;
            this.dispatchTimeMs = dispatchTimeMs;
        }
    }

    /** A tablet with too many segments for its size, found in one scan. */
    private static final class Candidate {
        final long dbId;
        final long tableId;
        final long partitionId;
        final long indexId;
        final long tabletId;
        final int schemaHash;
        final CloudReplica replica;
        final long segments;
        final long excessSegments;

        Candidate(long dbId, long tableId, long partitionId, long indexId, long tabletId, int schemaHash,
                CloudReplica replica, long segments, long excessSegments) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.indexId = indexId;
            this.tabletId = tabletId;
            this.schemaHash = schemaHash;
            this.replica = replica;
            this.segments = segments;
            this.excessSegments = excessSegments;
        }
    }

    // tablet id -> dispatched full compaction
    private final Map<Long, InFlight> inFlight = new HashMap<>();
    // tablet id -> time until which the tablet is not dispatched again
    private final Map<Long, Long> cooldownUntilMs = new HashMap<>();

    public FinalCompactionScheduler() {
        super("final-compaction-scheduler", intervalMs());
    }

    private static long intervalMs() {
        return Math.max(1L, Config.final_compaction_check_interval_second) * 1000L;
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(intervalMs());
        if (!Config.enable_final_compaction || !Config.isCloudMode()) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        expire(nowMs);

        Map<Long, CloudPartition> partitions = new HashMap<>();
        List<Candidate> candidates = collectCandidates(nowMs, partitions);
        if (candidates.isEmpty()) {
            return;
        }
        Set<Long> quiet = quietPartitionIds(partitions.values(), nowMs);
        candidates.removeIf(c -> !quiet.contains(c.partitionId));
        candidates.sort(Comparator.comparingLong((Candidate c) -> c.excessSegments).reversed());
        dispatch(candidates, nowMs);
    }

    /** Output segment size of the table's compactions, the size a merged tablet's segments reach. */
    @VisibleForTesting
    static long targetSegmentBytes(String compactionPolicy, Long timeSeriesGoalSizeMbytes,
            long defaultTargetMbytes) {
        long mbytes = defaultTargetMbytes;
        if (PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY.equals(compactionPolicy)
                && timeSeriesGoalSizeMbytes != null && timeSeriesGoalSizeMbytes > 0) {
            // A time series compaction cuts its output segments at twice the goal size.
            mbytes = timeSeriesGoalSizeMbytes * 2;
        }
        return Math.max(1L, mbytes) * 1024L * 1024L;
    }

    @VisibleForTesting
    static long idealSegments(long bytes, long targetBytes) {
        if (targetBytes <= 0) {
            return 1;
        }
        return Math.max(1L, (Math.max(0L, bytes) + targetBytes - 1) / targetBytes);
    }

    @VisibleForTesting
    static boolean isFragmented(long segments, long bytes, long targetBytes, double ratio, long minSegments) {
        return segments >= minSegments && segments >= ratio * idealSegments(bytes, targetBytes);
    }

    @VisibleForTesting
    static boolean isQuiet(long versionTimeMs, long nowMs, long quietMs) {
        return versionTimeMs > 0 && nowMs - versionTimeMs >= quietMs;
    }

    private List<Candidate> collectCandidates(long nowMs, Map<Long, CloudPartition> partitions) {
        double ratio = Config.final_compaction_segment_ratio;
        long minSegments = Config.final_compaction_min_segments;
        List<Candidate> candidates = new ArrayList<>();
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
                    if (olapTable.getState() != OlapTableState.NORMAL
                            || Boolean.TRUE.equals(olapTable.disableAutoCompaction())) {
                        continue;
                    }
                    long targetBytes = targetSegmentBytes(olapTable.getCompactionPolicy(),
                            olapTable.getTimeSeriesCompactionGoalSizeMbytes(),
                            Config.final_compaction_default_target_mbytes);
                    for (Partition partition : olapTable.getPartitions()) {
                        if (!(partition instanceof CloudPartition)) {
                            continue;
                        }
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            int schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
                            for (Tablet tablet : index.getTablets()) {
                                if (!isDispatchable(tablet.getId(), nowMs)) {
                                    continue;
                                }
                                for (Replica replica : tablet.getReplicas()) {
                                    if (!(replica instanceof CloudReplica)) {
                                        continue;
                                    }
                                    CloudReplica cloudReplica = (CloudReplica) replica;
                                    long segments = cloudReplica.getSegmentCount();
                                    long bytes = cloudReplica.getLocalSegmentSize() > 0
                                            ? cloudReplica.getLocalSegmentSize() : cloudReplica.getDataSize();
                                    if (!isFragmented(segments, bytes, targetBytes, ratio, minSegments)) {
                                        continue;
                                    }
                                    candidates.add(new Candidate(dbId, olapTable.getId(), partition.getId(),
                                            index.getId(), tablet.getId(), schemaHash, cloudReplica, segments,
                                            segments - idealSegments(bytes, targetBytes)));
                                    partitions.put(partition.getId(), (CloudPartition) partition);
                                }
                            }
                        }
                    }
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        return candidates;
    }

    private Set<Long> quietPartitionIds(Collection<CloudPartition> partitions, long nowMs) {
        long quietMs = Config.final_compaction_quiet_second * 1000L;
        List<CloudPartition> all = new ArrayList<>(partitions);
        Set<Long> quiet = new HashSet<>();
        for (int i = 0; i < all.size(); i += VERSION_BATCH_SIZE) {
            List<CloudPartition> batch = all.subList(i, Math.min(all.size(), i + VERSION_BATCH_SIZE));
            try {
                // Refreshes each partition's visible version and the time the meta service set it.
                CloudPartition.getSnapshotVisibleVersionFromMs(batch, false);
            } catch (RpcException e) {
                LOG.warn("final compaction: failed to get partition versions from meta service", e);
                continue;
            }
            for (CloudPartition partition : batch) {
                if (isQuiet(partition.getVisibleVersionTime(), nowMs, quietMs)) {
                    quiet.add(partition.getId());
                }
            }
        }
        return quiet;
    }

    private void dispatch(List<Candidate> candidates, long nowMs) {
        String clusterId = "";
        String computeGroup = Config.final_compaction_compute_group;
        if (!Strings.isNullOrEmpty(computeGroup)) {
            clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(computeGroup);
            if (Strings.isNullOrEmpty(clusterId)) {
                LOG.warn("final compaction: compute group {} not found", computeGroup);
                return;
            }
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Candidate c : candidates) {
            if (batchTask.getTaskNum() >= Config.final_compaction_max_dispatch_per_round) {
                break;
            }
            String tabletClusterId = clusterId.isEmpty() ? firstComputeGroup(c.replica) : clusterId;
            if (tabletClusterId == null) {
                continue;
            }
            long backendId;
            try {
                backendId = c.replica.getBackendIdWithClusterId(tabletClusterId);
            } catch (ComputeGroupException e) {
                LOG.debug("final compaction: no backend for tablet {}: {}", c.tabletId, e.getMessage());
                continue;
            }
            if (backendId <= 0 || !reserve(c.tabletId, backendId, nowMs)) {
                continue;
            }
            CompactionTask task = new CompactionTask(backendId, c.dbId, c.tableId, c.partitionId, c.indexId,
                    c.tabletId, c.schemaHash, "full");
            task.setReportFinish(true);
            batchTask.addTask(task);
            LOG.info("final compaction: dispatch full compaction of tablet {} with {} segments to backend {}",
                    c.tabletId, c.segments, backendId);
        }
        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
    }

    private static String firstComputeGroup(CloudReplica replica) {
        TreeSet<String> groups = new TreeSet<>(replica.getPrimaryComputeGroupIds());
        return groups.isEmpty() ? null : groups.first();
    }

    private synchronized boolean isDispatchable(long tabletId, long nowMs) {
        if (inFlight.containsKey(tabletId)) {
            return false;
        }
        Long until = cooldownUntilMs.get(tabletId);
        return until == null || until <= nowMs;
    }

    /** Records a dispatch, unless the tablet is already in flight or the backend is at its cap. */
    @VisibleForTesting
    synchronized boolean reserve(long tabletId, long backendId, long nowMs) {
        if (inFlight.containsKey(tabletId)) {
            return false;
        }
        long running = inFlight.values().stream().filter(f -> f.backendId == backendId).count();
        if (running >= Config.final_compaction_max_tasks_per_be) {
            return false;
        }
        inFlight.put(tabletId, new InFlight(backendId, nowMs));
        return true;
    }

    /** Called with a backend's report of the end of a compaction task, keyed by tablet id. */
    public synchronized void onTaskFinished(long backendId, long tabletId, TStatus status) {
        InFlight task = inFlight.get(tabletId);
        if (task == null || task.backendId != backendId) {
            return;
        }
        inFlight.remove(tabletId);
        long nowMs = System.currentTimeMillis();
        cooldownUntilMs.put(tabletId, nowMs + Config.final_compaction_cooldown_second * 1000L);
        boolean ok = status != null && status.getStatusCode() == TStatusCode.OK;
        if (ok) {
            LOG.info("final compaction: full compaction of tablet {} on backend {} finished in {} s",
                    tabletId, backendId, (nowMs - task.dispatchTimeMs) / 1000);
        } else {
            LOG.warn("final compaction: full compaction of tablet {} on backend {} failed: {}",
                    tabletId, backendId, status);
        }
    }

    /** Frees tasks whose end was never reported, and forgets elapsed cooldowns. */
    @VisibleForTesting
    synchronized void expire(long nowMs) {
        long timeoutMs = Config.final_compaction_task_timeout_second * 1000L;
        Iterator<Map.Entry<Long, InFlight>> it = inFlight.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, InFlight> entry = it.next();
            if (nowMs - entry.getValue().dispatchTimeMs >= timeoutMs) {
                LOG.warn("final compaction: no report of full compaction of tablet {} on backend {} after {} s",
                        entry.getKey(), entry.getValue().backendId, timeoutMs / 1000);
                cooldownUntilMs.put(entry.getKey(), nowMs + Config.final_compaction_cooldown_second * 1000L);
                it.remove();
            }
        }
        cooldownUntilMs.values().removeIf(until -> until <= nowMs);
    }

    @VisibleForTesting
    synchronized int inFlightCount(long backendId) {
        return (int) inFlight.values().stream().filter(f -> f.backendId == backendId).count();
    }

    @VisibleForTesting
    synchronized boolean inCooldown(long tabletId, long nowMs) {
        Long until = cooldownUntilMs.get(tabletId);
        return until != null && until > nowMs;
    }
}
