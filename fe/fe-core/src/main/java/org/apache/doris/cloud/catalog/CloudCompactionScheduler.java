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

import org.apache.doris.catalog.CloudTabletStatMgr;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Sends compactions to backends for tablets their own schedulers miss. A backend only picks
 * compaction candidates among the tablets it has cached, so the tablets that nothing loads or
 * reads after it restarts keep their rowset backlog, and nothing merges the small segments
 * cumulative compaction leaves on a partition once loads into it stop.
 *
 * <p>Two rules, each with its own switch, per-backend cap and cooldown:
 * <ul>
 * <li>Compaction debt ({@link Config#enable_compaction_debt_dispatch}): a tablet with at least
 * {@link Config#compaction_debt_min_rowsets} rowsets and no cumulative compaction committed for
 * {@link Config#compaction_debt_idle_second} gets a cumulative compaction, largest backlog first,
 * on its backend in {@link Config#compaction_debt_compute_group}, or in the compute group that
 * last loaded or compacted it. The backend loads the tablet to run it, and keeps compacting it
 * from then on.</li>
 * <li>Final compaction ({@link Config#enable_final_compaction}): a tablet with at least
 * {@link Config#final_compaction_segment_ratio} times the segments its size needs at the table's
 * output segment size, in a partition with no new visible version for
 * {@link Config#final_compaction_quiet_second}, gets a full compaction.</li>
 * </ul>
 * The task asks the backend to report its end; until then, and until its cooldown passes, the
 * tablet is not sent another. All state is in memory on the master.
 */
public class CloudCompactionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(CloudCompactionScheduler.class);

    private static final int VERSION_BATCH_SIZE = 500;

    /** The rule a compaction was sent by. */
    @VisibleForTesting
    enum Kind {
        DEBT("cumulative"),
        FINAL("full");

        final String compactionType;

        Kind(String compactionType) {
            this.compactionType = compactionType;
        }

        int maxTasksPerBackend() {
            return this == DEBT ? Config.compaction_debt_max_tasks_per_be : Config.final_compaction_max_tasks_per_be;
        }

        int maxDispatchPerRound() {
            return this == DEBT ? Config.compaction_debt_max_dispatch_per_round
                    : Config.final_compaction_max_dispatch_per_round;
        }

        long timeoutMs() {
            return (this == DEBT ? Config.compaction_debt_task_timeout_second
                    : Config.final_compaction_task_timeout_second) * 1000L;
        }

        long cooldownMs() {
            return (this == DEBT ? Config.compaction_debt_cooldown_second
                    : Config.final_compaction_cooldown_second) * 1000L;
        }
    }

    /** A compaction this scheduler sent whose end has not been reported yet. */
    @VisibleForTesting
    static final class InFlight {
        final Kind kind;
        final long backendId;
        final long dispatchTimeMs;

        InFlight(Kind kind, long backendId, long dispatchTimeMs) {
            this.kind = kind;
            this.backendId = backendId;
            this.dispatchTimeMs = dispatchTimeMs;
        }
    }

    /** A tablet one of the rules picked in a scan. */
    private static final class Candidate {
        final Kind kind;
        final long dbId;
        final long tableId;
        final long partitionId;
        final long indexId;
        final long tabletId;
        final int schemaHash;
        final CloudReplica replica;
        // How far the tablet is from what the rule aims for; larger is sent first.
        final long excess;

        Candidate(Kind kind, long dbId, long tableId, long partitionId, long indexId, long tabletId,
                int schemaHash, CloudReplica replica, long excess) {
            this.kind = kind;
            this.dbId = dbId;
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.indexId = indexId;
            this.tabletId = tabletId;
            this.schemaHash = schemaHash;
            this.replica = replica;
            this.excess = excess;
        }
    }

    // tablet id -> sent compaction
    private final Map<Long, InFlight> inFlight = new HashMap<>();
    // tablet id -> time until which the tablet is not sent another compaction
    private final Map<Long, Long> cooldownUntilMs = new HashMap<>();

    public CloudCompactionScheduler() {
        super("cloud-compaction-scheduler", intervalMs());
    }

    private static long intervalMs() {
        return Math.max(1L, Config.final_compaction_check_interval_second) * 1000L;
    }

    @Override
    protected void runAfterCatalogReady() {
        setInterval(intervalMs());
        if (!Config.isCloudMode() || (!Config.enable_final_compaction && !Config.enable_compaction_debt_dispatch)) {
            return;
        }
        long nowMs = System.currentTimeMillis();
        expire(nowMs);

        List<Candidate> debt = new ArrayList<>();
        List<Candidate> fragmented = new ArrayList<>();
        Map<Long, CloudPartition> fragmentedPartitions = new HashMap<>();
        collectCandidates(nowMs, debt, fragmented, fragmentedPartitions);

        if (!debt.isEmpty()) {
            debt.sort(Comparator.comparingLong((Candidate c) -> c.excess).reversed());
            dispatch(debt, nowMs, Config.compaction_debt_compute_group, true);
        }
        if (!fragmented.isEmpty()) {
            Set<Long> quiet = quietPartitionIds(fragmentedPartitions.values(), nowMs);
            fragmented.removeIf(c -> !quiet.contains(c.partitionId));
            fragmented.sort(Comparator.comparingLong((Candidate c) -> c.excess).reversed());
            dispatch(fragmented, nowMs, Config.final_compaction_compute_group, false);
        }
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

    /** True when the tablet has a rowset backlog that cumulative compaction has left alone. */
    @VisibleForTesting
    static boolean hasCompactionDebt(long rowsets, long lastCumuCompactionTimeMs, long nowMs,
            long minRowsets, long idleMs) {
        return rowsets >= minRowsets && nowMs - Math.max(0L, lastCumuCompactionTimeMs) >= idleMs;
    }

    private void collectCandidates(long nowMs, List<Candidate> debt, List<Candidate> fragmented,
            Map<Long, CloudPartition> fragmentedPartitions) {
        final boolean collectDebt = Config.enable_compaction_debt_dispatch;
        final boolean collectFinal = Config.enable_final_compaction;
        final long minRowsets = Math.max(2L, Config.compaction_debt_min_rowsets);
        final long idleMs = Config.compaction_debt_idle_second * 1000L;
        final double ratio = Config.final_compaction_segment_ratio;
        final long minSegments = Config.final_compaction_min_segments;
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
                                    long rowsets = cloudReplica.getRowsetCount();
                                    if (collectDebt && hasCompactionDebt(rowsets,
                                            cloudReplica.getLastCumuCompactionTimeMs(), nowMs, minRowsets, idleMs)) {
                                        debt.add(new Candidate(Kind.DEBT, dbId, olapTable.getId(), partition.getId(),
                                                index.getId(), tablet.getId(), schemaHash, cloudReplica,
                                                rowsets - minRowsets));
                                        continue;
                                    }
                                    if (!collectFinal) {
                                        continue;
                                    }
                                    long segments = cloudReplica.getSegmentCount();
                                    long bytes = cloudReplica.getLocalSegmentSize() > 0
                                            ? cloudReplica.getLocalSegmentSize() : cloudReplica.getDataSize();
                                    if (isFragmented(segments, bytes, targetBytes, ratio, minSegments)) {
                                        fragmented.add(new Candidate(Kind.FINAL, dbId, olapTable.getId(),
                                                partition.getId(), index.getId(), tablet.getId(), schemaHash,
                                                cloudReplica, segments - idealSegments(bytes, targetBytes)));
                                        fragmentedPartitions.put(partition.getId(), (CloudPartition) partition);
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
                LOG.warn("compaction scheduler: failed to get partition versions from meta service", e);
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

    /**
     * Sends the candidates, in order, to their backends. With no compute group configured, a debt
     * compaction goes to the compute group that last loaded or compacted the tablet, and a final
     * compaction to a compute group the tablet is mapped to.
     */
    private void dispatch(List<Candidate> candidates, long nowMs, String computeGroup, boolean useLastActive) {
        String clusterId = "";
        if (!Strings.isNullOrEmpty(computeGroup)) {
            clusterId = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterIdByName(computeGroup);
            if (Strings.isNullOrEmpty(clusterId)) {
                LOG.warn("compaction scheduler: compute group {} not found", computeGroup);
                return;
            }
        }
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Candidate c : candidates) {
            if (batchTask.getTaskNum() >= c.kind.maxDispatchPerRound()) {
                break;
            }
            String tabletClusterId = clusterId;
            if (tabletClusterId.isEmpty() && useLastActive) {
                tabletClusterId = c.replica.getLastActiveClusterId();
            }
            if (Strings.isNullOrEmpty(tabletClusterId)) {
                tabletClusterId = firstComputeGroup(c.replica);
            }
            if (tabletClusterId == null) {
                continue;
            }
            long backendId;
            try {
                backendId = c.replica.getBackendIdWithClusterId(tabletClusterId);
            } catch (ComputeGroupException e) {
                LOG.debug("compaction scheduler: no backend for tablet {}: {}", c.tabletId, e.getMessage());
                continue;
            }
            if (backendId <= 0 || !reserve(c.kind, c.tabletId, backendId, nowMs)) {
                continue;
            }
            CompactionTask task = new CompactionTask(backendId, c.dbId, c.tableId, c.partitionId, c.indexId,
                    c.tabletId, c.schemaHash, c.kind.compactionType);
            task.setReportFinish(true);
            batchTask.addTask(task);
            LOG.info("compaction scheduler: send {} compaction of tablet {} to backend {}, excess {}",
                    c.kind.compactionType, c.tabletId, backendId, c.excess);
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

    /** Records a dispatch, unless the tablet is in flight or the backend is at the rule's cap. */
    @VisibleForTesting
    synchronized boolean reserve(Kind kind, long tabletId, long backendId, long nowMs) {
        if (inFlight.containsKey(tabletId)) {
            return false;
        }
        long running = inFlight.values().stream().filter(f -> f.kind == kind && f.backendId == backendId).count();
        if (running >= kind.maxTasksPerBackend()) {
            return false;
        }
        inFlight.put(tabletId, new InFlight(kind, backendId, nowMs));
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
        cooldownUntilMs.put(tabletId, nowMs + task.kind.cooldownMs());
        // Fetch the tablet's stats soon, so the next scans see what the compaction did.
        Env env = Env.getCurrentEnv();
        if (env != null && env.getTabletStatMgr() instanceof CloudTabletStatMgr) {
            ((CloudTabletStatMgr) env.getTabletStatMgr()).addActiveTablets(Collections.singletonList(tabletId));
        }
        boolean ok = status != null && status.getStatusCode() == TStatusCode.OK;
        if (ok) {
            LOG.info("compaction scheduler: {} compaction of tablet {} on backend {} finished in {} s",
                    task.kind.compactionType, tabletId, backendId, (nowMs - task.dispatchTimeMs) / 1000);
        } else {
            LOG.warn("compaction scheduler: {} compaction of tablet {} on backend {} failed: {}",
                    task.kind.compactionType, tabletId, backendId, status);
        }
    }

    /** Frees tasks whose end was never reported, and forgets elapsed cooldowns. */
    @VisibleForTesting
    synchronized void expire(long nowMs) {
        Iterator<Map.Entry<Long, InFlight>> it = inFlight.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, InFlight> entry = it.next();
            InFlight task = entry.getValue();
            if (nowMs - task.dispatchTimeMs >= task.kind.timeoutMs()) {
                LOG.warn("compaction scheduler: no report of {} compaction of tablet {} on backend {} after {} s",
                        task.kind.compactionType, entry.getKey(), task.backendId, task.kind.timeoutMs() / 1000);
                cooldownUntilMs.put(entry.getKey(), nowMs + task.kind.cooldownMs());
                it.remove();
            }
        }
        cooldownUntilMs.values().removeIf(until -> until <= nowMs);
    }

    @VisibleForTesting
    synchronized int inFlightCount(Kind kind, long backendId) {
        return (int) inFlight.values().stream().filter(f -> f.kind == kind && f.backendId == backendId).count();
    }

    @VisibleForTesting
    synchronized boolean inCooldown(long tabletId, long nowMs) {
        Long until = cooldownUntilMs.get(tabletId);
        return until != null && until > nowMs;
    }
}
