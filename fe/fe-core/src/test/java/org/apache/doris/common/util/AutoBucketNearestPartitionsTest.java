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
import org.apache.doris.catalog.Type;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.AutoBucketCalculator.PartitionStat;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AutoBucketNearestPartitionsTest {
    private static final long DAY = 86400L;
    private static final long GB = 1024L * 1024 * 1024;

    private int savedWindow;
    private int savedMinBuckets;

    @BeforeEach
    public void setUp() {
        savedWindow = Config.autobucket_nearest_partitions_window;
        savedMinBuckets = Config.autobucket_min_buckets;
        Config.autobucket_nearest_partitions_window = 7;
        Config.autobucket_min_buckets = 3;
        // Without backends the disk capacity would cap every estimate.
        new MockUp<AutoBucketUtils>() {
            @Mock
            public int getBucketsNumByBEDisks() {
                return Integer.MAX_VALUE;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        Config.autobucket_nearest_partitions_window = savedWindow;
        Config.autobucket_min_buckets = savedMinBuckets;
    }

    private static PartitionStat day(long day, long size, int buckets) {
        return new PartitionStat("p" + day, day * DAY, size, buckets);
    }

    private static List<Long> days(List<PartitionStat> stats) {
        return stats.stream().map(p -> p.start / DAY).collect(Collectors.toList());
    }

    private static List<PartitionStat> bigRecentDays() {
        List<PartitionStat> stats = new ArrayList<>();
        for (long d = 1000; d <= 1006; d++) {
            stats.add(day(d, 200 * GB, 64));
        }
        return stats;
    }

    @Test
    public void testHistoricalDayFarFromDataGetsMinBuckets() {
        List<PartitionStat> stats = bigRecentDays();
        long newStart = 10 * DAY;
        Assertions.assertFalse(AutoBucketCalculator.isAtNewestEnd(stats, newStart));
        List<PartitionStat> nearest = AutoBucketCalculator.nearestPartitions(stats, newStart, DAY);
        Assertions.assertTrue(nearest.isEmpty());
        Pair<Integer, Integer> buckets = AutoBucketCalculator.bucketsFromNearestPartitions(nearest);
        Assertions.assertEquals(3, buckets.first);
        // Nothing for the bounds check to correct.
        Assertions.assertEquals(-1, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(
                buckets.first, buckets.second, "t", "p10"));
    }

    @Test
    public void testDayNextToBigDaysGetsTheirSize() {
        List<PartitionStat> stats = new ArrayList<>();
        for (long d = 0; d < 5; d++) {
            stats.add(day(d, 1024 * 1024, 3));
        }
        for (long d = 1000; d <= 1006; d++) {
            // The neighbours' own bucket counts do not bound the estimate.
            stats.add(day(d, 200 * GB, 3));
        }
        long newStart = 999 * DAY;
        List<PartitionStat> nearest = AutoBucketCalculator.nearestPartitions(stats, newStart, DAY);
        Assertions.assertEquals(Arrays.asList(1000L, 1001L, 1002L, 1003L, 1004L, 1005L, 1006L), days(nearest));
        int expected = DynamicPartitionScheduler.estimateBucketsNum(Collections.nCopies(7, 200 * GB));
        Assertions.assertTrue(expected > Config.autobucket_min_buckets);
        Pair<Integer, Integer> buckets = AutoBucketCalculator.bucketsFromNearestPartitions(nearest);
        Assertions.assertEquals(expected, buckets.first);
        Assertions.assertEquals(-1, DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(
                buckets.first, buckets.second, "t", "p999"));
    }

    @Test
    public void testNewestDayKeepsTheNewestPartitionsEstimate() {
        List<PartitionStat> stats = bigRecentDays();
        // At the newest end the existing estimate from the newest partitions is used.
        Assertions.assertTrue(AutoBucketCalculator.isAtNewestEnd(stats, 1007 * DAY));
        Assertions.assertTrue(AutoBucketCalculator.isAtNewestEnd(stats, 1100 * DAY));
        Assertions.assertFalse(AutoBucketCalculator.isAtNewestEnd(stats, 1003 * DAY + 3600));
        Assertions.assertFalse(AutoBucketCalculator.isAtNewestEnd(stats, 999 * DAY));
    }

    @Test
    public void testGapWithinWindow() {
        List<PartitionStat> stats = new ArrayList<>(Arrays.asList(day(900, 10 * GB, 8), day(995, 100 * GB, 32),
                day(998, 100 * GB, 32), day(1003, 100 * GB, 32), day(1020, GB, 3)));
        long newStart = 1000 * DAY;
        Assertions.assertEquals(Arrays.asList(995L, 998L, 1003L),
                days(AutoBucketCalculator.nearestPartitions(stats, newStart, DAY)));
        Config.autobucket_nearest_partitions_window = 2;
        Assertions.assertEquals(Arrays.asList(998L), days(AutoBucketCalculator.nearestPartitions(stats, newStart, DAY)));
        Config.autobucket_nearest_partitions_window = 0;
        List<PartitionStat> none = AutoBucketCalculator.nearestPartitions(stats, newStart, DAY);
        Assertions.assertTrue(none.isEmpty());
        Assertions.assertEquals(3, AutoBucketCalculator.bucketsFromNearestPartitions(none).first);
    }

    @Test
    public void testClosestPartitionsInRangeOrder() {
        List<PartitionStat> stats = new ArrayList<>();
        for (long d = 990; d <= 1010; d++) {
            if (d != 1000) {
                stats.add(day(d, GB, 3));
            }
        }
        Assertions.assertEquals(Arrays.asList(996L, 997L, 998L, 999L, 1001L, 1002L, 1003L),
                days(AutoBucketCalculator.nearestPartitions(stats, 1000 * DAY, DAY)));
    }

    @Test
    public void testNeighboursOfUnknownSizeAreLeftOut() {
        // Days created moments earlier by the same historical load, with many buckets.
        List<PartitionStat> nearest = Arrays.asList(day(995, 0, 64), day(999, 0, 64));
        Pair<Integer, Integer> buckets = AutoBucketCalculator.bucketsFromNearestPartitions(nearest);
        Assertions.assertEquals(3, buckets.first);
        Assertions.assertEquals(3, buckets.second);
        List<PartitionStat> mixed = Arrays.asList(day(995, 0, 64), day(999, 200 * GB, 64));
        Assertions.assertEquals(DynamicPartitionScheduler.estimateBucketsNum(Collections.singletonList(200 * GB)),
                AutoBucketCalculator.bucketsFromNearestPartitions(mixed).first);
    }

    @Test
    public void testKeyOrdinalOfDatesAndDatetimes() throws Exception {
        Long day1 = AutoBucketCalculator.keyOrdinal(new DateLiteral("2022-01-01", Type.DATEV2));
        Long day2 = AutoBucketCalculator.keyOrdinal(new DateLiteral("2022-01-02", Type.DATEV2));
        Assertions.assertNotNull(day1);
        Assertions.assertEquals(DAY, day2 - day1);
        Long dt1 = AutoBucketCalculator.keyOrdinal(new DateLiteral("2022-01-01 00:00:00", Type.DATETIMEV2));
        Long dt2 = AutoBucketCalculator.keyOrdinal(new DateLiteral("2022-01-01 06:00:00", Type.DATETIMEV2));
        Assertions.assertEquals(day1, dt1);
        Assertions.assertEquals(6 * 3600L, dt2 - dt1);
    }
}
