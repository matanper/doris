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

package org.apache.doris.common.util;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for calculating auto bucket numbers for partitions.
 * This class provides a unified interface for auto bucket calculation
 * used by both DynamicPartitionScheduler and PartitionExprUtil.
 */
public class AutoBucketCalculator {
    private static final Logger LOG = LogManager.getLogger(AutoBucketCalculator.class);

    /**
     * Context for auto bucket calculation
     */
    public static class AutoBucketContext {
        private final OlapTable table;
        private final String partitionName;
        private final String nowPartitionName;
        private final boolean executeFirstTime;
        private final int defaultBuckets;
        // Range of the new partition on the scale of keyOrdinal, when it is known.
        private final Long newPartitionStart;
        private final Long newPartitionEnd;

        public AutoBucketContext(OlapTable table, String partitionName, String nowPartitionName,
                                boolean executeFirstTime, int defaultBuckets) {
            this(table, partitionName, nowPartitionName, executeFirstTime, defaultBuckets, null, null);
        }

        public AutoBucketContext(OlapTable table, String partitionName, String nowPartitionName,
                                boolean executeFirstTime, int defaultBuckets, Long newPartitionStart,
                                Long newPartitionEnd) {
            this.table = table;
            this.partitionName = partitionName;
            this.nowPartitionName = nowPartitionName;
            this.executeFirstTime = executeFirstTime;
            this.defaultBuckets = defaultBuckets;
            this.newPartitionStart = newPartitionStart;
            this.newPartitionEnd = newPartitionEnd;
        }

        public Long getNewPartitionStart() {
            return newPartitionStart;
        }

        public Long getNewPartitionEnd() {
            return newPartitionEnd;
        }

        public OlapTable getTable() {
            return table;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public String getNowPartitionName() {
            return nowPartitionName;
        }

        public boolean isExecuteFirstTime() {
            return executeFirstTime;
        }

        public int getDefaultBuckets() {
            return defaultBuckets;
        }
    }

    /**
     * Result of auto bucket calculation
     */
    public static class AutoBucketResult {
        private final int buckets;
        private final int previousBuckets;
        private final boolean success;
        private final String errorMessage;

        private AutoBucketResult(int buckets, int previousBuckets, boolean success, String errorMessage) {
            this.buckets = buckets;
            this.previousBuckets = previousBuckets;
            this.success = success;
            this.errorMessage = errorMessage;
        }

        public static AutoBucketResult success(int buckets, int previousBuckets) {
            return new AutoBucketResult(buckets, previousBuckets, true, null);
        }

        public static AutoBucketResult fallback(int defaultBuckets, String reason) {
            return new AutoBucketResult(defaultBuckets, 0, false, reason);
        }

        public int getBuckets() {
            return buckets;
        }

        public int getPreviousBuckets() {
            return previousBuckets;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Calculate auto bucket number for a partition.
     * This is the unified method that replaces the logic in both
     * DynamicPartitionScheduler.getBucketsNum and PartitionExprUtil.getAddPartitionClauseFromPartitionValues
     *
     * @param context the context for auto bucket calculation
     * @return AutoBucketResult containing bucket numbers and calculation status
     */
    public static AutoBucketResult calculateAutoBuckets(AutoBucketContext context) {
        OlapTable table = context.getTable();
        String partitionName = context.getPartitionName();
        String nowPartitionName = context.getNowPartitionName();
        boolean executeFirstTime = context.isExecuteFirstTime();
        int defaultBuckets = context.getDefaultBuckets();

        // if execute first time or not auto bucket, use default buckets
        if (!table.isAutoBucket() || executeFirstTime) {
            return AutoBucketResult.fallback(defaultBuckets,
                executeFirstTime ? "executeFirstTime" : "not auto bucket table");
        }

        // Get historical partitions
        List<Partition> partitions = DynamicPartitionScheduler.getHistoricalPartitions(table, nowPartitionName);

        // Get visible versions with error handling
        List<Long> visibleVersions;
        try {
            visibleVersions = Partition.getVisibleVersions(partitions);
        } catch (RpcException e) {
            LOG.warn("auto bucket get visible version fail, table: [{}-{}], partition: {}, use default buckets: {}",
                    table.getName(), table.getId(), partitionName, defaultBuckets, e);
            return AutoBucketResult.fallback(defaultBuckets, "RpcException: " + e.getMessage());
        }

        // Check if visible versions match partitions
        if (visibleVersions == null || partitions.size() != visibleVersions.size()) {
            LOG.warn(
                    "auto bucket visible versions mismatch, table: [{}-{}], partition: {}, "
                            + "partitions size: {}, visible versions size: {}, use default buckets: {}",
                    table.getName(), table.getId(), partitionName, partitions.size(),
                    visibleVersions != null ? visibleVersions.size() : 0, defaultBuckets);
            return AutoBucketResult.fallback(defaultBuckets, "visible versions mismatch");
        }

        // Filter partitions with data
        List<Partition> hasDataPartitions = DynamicPartitionScheduler.filterDataPartitions(partitions, visibleVersions);
        if (hasDataPartitions.isEmpty()) {
            LOG.info(
                    "auto bucket use default buckets due to all partitions no data, table: [{}-{}], "
                            + "partition: {}, buckets num: {}",
                    table.getName(), table.getId(), partitionName, defaultBuckets);
            return AutoBucketResult.fallback(defaultBuckets, "no data partitions");
        }

        Pair<Integer, Integer> nearest = calculateBucketsFromNearestPartitions(context, hasDataPartitions);
        if (nearest != null) {
            return AutoBucketResult.success(nearest.first, nearest.second);
        }

        // Calculate buckets based on historical data
        Pair<Integer, Integer> calc = DynamicPartitionScheduler.calculateBuckets(hasDataPartitions);
        int candidateBuckets = calc.first;
        int previousBuckets = calc.second;

        return AutoBucketResult.success(candidateBuckets, previousBuckets);
    }

    /**
     * The start of a partition with data and what the estimate needs of it.
     */
    @VisibleForTesting
    public static final class PartitionStat {
        final String name;
        final long start;
        final long dataSize;
        final int buckets;

        public PartitionStat(String name, long start, long dataSize, int buckets) {
            this.name = name;
            this.start = start;
            this.dataSize = dataSize;
            this.buckets = buckets;
        }
    }

    private static final int NEAREST_PARTITIONS = 7;

    /**
     * The position of a single-column range partition key on a linear scale (seconds for date and
     * datetime keys), or null when the key has none.
     */
    public static Long keyOrdinal(PartitionKey key) {
        if (key == null || key.isMinValue() || key.isMaxValue() || key.getKeys().size() != 1) {
            return null;
        }
        return keyOrdinal(key.getKeys().get(0));
    }

    public static Long keyOrdinal(LiteralExpr key) {
        if (key instanceof DateLiteral) {
            DateLiteral date = (DateLiteral) key;
            try {
                return LocalDateTime.of((int) date.getYear(), (int) date.getMonth(), (int) date.getDay(),
                        (int) date.getHour(), (int) date.getMinute(), (int) date.getSecond())
                        .toEpochSecond(ZoneOffset.UTC);
            } catch (DateTimeException e) {
                return null;
            }
        }
        if (key instanceof IntLiteral) {
            return key.getLongValue();
        }
        return null;
    }

    /**
     * Buckets of a new range partition from the partitions with data nearest to it, or null to
     * use the newest partitions instead.
     *
     * The newest partitions predict the next one when data arrives in range order, which is what
     * they are used for. A partition created further back, as when historical data is loaded,
     * looks like its neighbours rather than like the newest partitions, so it is estimated from
     * the partitions with data that start within autobucket_nearest_partitions_window partition
     * intervals of it, and gets autobucket_min_buckets when none of them has a known size.
     */
    private static Pair<Integer, Integer> calculateBucketsFromNearestPartitions(AutoBucketContext context,
            List<Partition> hasDataPartitions) {
        Long newStart = context.getNewPartitionStart();
        Long newEnd = context.getNewPartitionEnd();
        if (!Config.autobucket_use_nearest_partitions || newStart == null || newEnd == null || newEnd <= newStart) {
            return null;
        }
        List<PartitionStat> stats = partitionStats(context.getTable(), hasDataPartitions);
        if (stats == null || isAtNewestEnd(stats, newStart)) {
            return null;
        }
        List<PartitionStat> nearest = nearestPartitions(stats, newStart, newEnd - newStart);
        Pair<Integer, Integer> buckets = bucketsFromNearestPartitions(nearest);
        LOG.info("auto bucket calc from {} nearest partitions, table: [{}-{}], partition: {}, buckets num: {}, "
                        + "nearest partitions: {}",
                nearest.size(), context.getTable().getName(), context.getTable().getId(),
                context.getPartitionName(), buckets.first,
                nearest.stream().map(p -> "(" + p.name + ", " + p.dataSize + ")").collect(Collectors.toList()));
        return buckets;
    }

    private static List<PartitionStat> partitionStats(OlapTable table, List<Partition> partitions) {
        List<PartitionStat> stats = new ArrayList<>(partitions.size());
        table.readLock();
        try {
            for (Partition partition : partitions) {
                PartitionItem item = table.getPartitionInfo().getItem(partition.getId());
                if (!(item instanceof RangePartitionItem)) {
                    return null;
                }
                Long start = keyOrdinal(((RangePartitionItem) item).getItems().lowerEndpoint());
                if (start == null) {
                    return null;
                }
                stats.add(new PartitionStat(partition.getName(), start,
                        partition.getDataSizeExcludeEmptyReplica(true),
                        partition.getDistributionInfo().getBucketNum()));
            }
        } finally {
            table.readUnlock();
        }
        return stats;
    }

    @VisibleForTesting
    public static boolean isAtNewestEnd(List<PartitionStat> stats, long newStart) {
        return stats.stream().allMatch(p -> p.start < newStart);
    }

    /**
     * The partitions starting within autobucket_nearest_partitions_window intervals of newStart,
     * the closest NEAREST_PARTITIONS of them, in range order.
     */
    @VisibleForTesting
    public static List<PartitionStat> nearestPartitions(List<PartitionStat> stats, long newStart, long interval) {
        long window = Math.max(0, Config.autobucket_nearest_partitions_window) * interval;
        return stats.stream()
                .filter(p -> Math.abs(p.start - newStart) <= window)
                .sorted(Comparator.comparingLong((PartitionStat p) -> Math.abs(p.start - newStart))
                        .thenComparingLong(p -> p.start))
                .limit(NEAREST_PARTITIONS)
                .sorted(Comparator.comparingLong(p -> p.start))
                .collect(Collectors.toList());
    }

    /**
     * Buckets estimated from the sizes of the given partitions, in range order, paired with
     * themselves as the previous buckets: a neighbour may itself have been estimated from
     * partitions far away, so neither its buckets nor its size bound the result. Partitions whose
     * size is not known yet, such as neighbours created moments earlier by the same load, are left
     * out, and with no known size the new partition gets autobucket_min_buckets.
     */
    @VisibleForTesting
    public static Pair<Integer, Integer> bucketsFromNearestPartitions(List<PartitionStat> nearest) {
        List<Long> sizes = nearest.stream().map(p -> p.dataSize).filter(size -> size > 0)
                .collect(Collectors.toList());
        if (sizes.isEmpty()) {
            return Pair.of(Config.autobucket_min_buckets, Config.autobucket_min_buckets);
        }
        int buckets = DynamicPartitionScheduler.estimateBucketsNum(sizes);
        return Pair.of(buckets, buckets);
    }

    /**
     * Calculate auto bucket number and apply bounds checking.
     * This method combines calculateAutoBuckets with checkAndFixAutoBucketCalcNumIsValid.
     *
     * @param context the context for auto bucket calculation
     * @return final bucket number after bounds checking
     */
    public static int calculateAutoBucketsWithBoundsCheck(AutoBucketContext context) {
        AutoBucketResult result = calculateAutoBuckets(context);

        if (!result.isSuccess()) {
            return result.getBuckets(); // return default buckets
        }

        int candidateBuckets = result.getBuckets();
        int previousBuckets = result.getPreviousBuckets();

        // Apply bounds checking
        int adjustedBuckets = DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(
                candidateBuckets, previousBuckets, context.getTable().getName(), context.getPartitionName());

        return adjustedBuckets > 0 ? adjustedBuckets : candidateBuckets;
    }
}
