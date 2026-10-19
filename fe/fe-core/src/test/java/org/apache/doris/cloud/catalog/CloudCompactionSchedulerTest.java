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

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CloudCompactionSchedulerTest {
    private static final CloudCompactionScheduler.Kind FINAL = CloudCompactionScheduler.Kind.FINAL;
    private static final CloudCompactionScheduler.Kind DEBT = CloudCompactionScheduler.Kind.DEBT;
    private static final long MB = 1024L * 1024L;

    private int oldMaxTasksPerBe;
    private int oldDebtMaxTasksPerBe;
    private long oldDebtCooldownSecond;
    private long oldTimeoutSecond;
    private long oldCooldownSecond;

    @BeforeEach
    public void setUp() {
        oldMaxTasksPerBe = Config.final_compaction_max_tasks_per_be;
        oldDebtMaxTasksPerBe = Config.compaction_debt_max_tasks_per_be;
        oldDebtCooldownSecond = Config.compaction_debt_cooldown_second;
        oldTimeoutSecond = Config.final_compaction_task_timeout_second;
        oldCooldownSecond = Config.final_compaction_cooldown_second;
    }

    @AfterEach
    public void tearDown() {
        Config.final_compaction_max_tasks_per_be = oldMaxTasksPerBe;
        Config.compaction_debt_max_tasks_per_be = oldDebtMaxTasksPerBe;
        Config.compaction_debt_cooldown_second = oldDebtCooldownSecond;
        Config.final_compaction_task_timeout_second = oldTimeoutSecond;
        Config.final_compaction_cooldown_second = oldCooldownSecond;
    }

    @Test
    public void testTargetSegmentBytesFollowsTablePolicy() {
        Assertions.assertEquals(2048 * MB,
                CloudCompactionScheduler.targetSegmentBytes("time_series", 1024L, 1024));
        Assertions.assertEquals(1024 * MB,
                CloudCompactionScheduler.targetSegmentBytes("size_based", 1024L, 1024));
        Assertions.assertEquals(1024 * MB,
                CloudCompactionScheduler.targetSegmentBytes("time_series", null, 1024));
        Assertions.assertEquals(512 * MB,
                CloudCompactionScheduler.targetSegmentBytes(null, null, 512));
    }

    @Test
    public void testIdealSegments() {
        Assertions.assertEquals(1, CloudCompactionScheduler.idealSegments(0, 1024 * MB));
        Assertions.assertEquals(1, CloudCompactionScheduler.idealSegments(1024 * MB, 1024 * MB));
        Assertions.assertEquals(2, CloudCompactionScheduler.idealSegments(1024 * MB + 1, 1024 * MB));
        Assertions.assertEquals(7, CloudCompactionScheduler.idealSegments(13_630 * MB, 2048 * MB));
    }

    @Test
    public void testIsFragmented() {
        long target = 2048 * MB;
        // The dc1 tablet: 275 segments, 13.6 GB of segment data, 7 segments ideal.
        Assertions.assertTrue(CloudCompactionScheduler.isFragmented(275, 13_630 * MB, target, 4.0, 16));
        Assertions.assertFalse(CloudCompactionScheduler.isFragmented(27, 13_630 * MB, target, 4.0, 16));
        Assertions.assertTrue(CloudCompactionScheduler.isFragmented(28, 13_630 * MB, target, 4.0, 16));
        // Small tablets need the minimum segment count too.
        Assertions.assertFalse(CloudCompactionScheduler.isFragmented(15, 10 * MB, target, 4.0, 16));
        Assertions.assertTrue(CloudCompactionScheduler.isFragmented(16, 10 * MB, target, 4.0, 16));
        // No stats yet.
        Assertions.assertFalse(CloudCompactionScheduler.isFragmented(-1, 0, target, 4.0, 16));
    }

    @Test
    public void testIsQuiet() {
        long now = 10_000_000L;
        Assertions.assertTrue(CloudCompactionScheduler.isQuiet(now - 7_200_000L, now, 7_200_000L));
        Assertions.assertFalse(CloudCompactionScheduler.isQuiet(now - 7_199_999L, now, 7_200_000L));
        // Unknown version time.
        Assertions.assertFalse(CloudCompactionScheduler.isQuiet(0, now, 7_200_000L));
    }

    @Test
    public void testReserveCapsTasksPerBackend() {
        Config.final_compaction_max_tasks_per_be = 2;
        CloudCompactionScheduler scheduler = new CloudCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(FINAL, 1, 100, 0));
        Assertions.assertFalse(scheduler.reserve(FINAL, 1, 100, 0));
        Assertions.assertTrue(scheduler.reserve(FINAL, 2, 100, 0));
        Assertions.assertFalse(scheduler.reserve(FINAL, 3, 100, 0));
        Assertions.assertTrue(scheduler.reserve(FINAL, 3, 200, 0));
        Assertions.assertEquals(2, scheduler.inFlightCount(FINAL, 100));
        Assertions.assertEquals(1, scheduler.inFlightCount(FINAL, 200));
    }

    @Test
    public void testReportFreesSlotAndStartsCooldown() {
        Config.final_compaction_max_tasks_per_be = 1;
        Config.final_compaction_cooldown_second = 3600;
        CloudCompactionScheduler scheduler = new CloudCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(FINAL, 1, 100, 0));

        // A report from another backend is not about this dispatch.
        scheduler.onTaskFinished(200, 1, new TStatus(TStatusCode.OK));
        Assertions.assertEquals(1, scheduler.inFlightCount(FINAL, 100));

        scheduler.onTaskFinished(100, 1, new TStatus(TStatusCode.CANCELLED));
        Assertions.assertEquals(0, scheduler.inFlightCount(FINAL, 100));
        long now = System.currentTimeMillis();
        Assertions.assertTrue(scheduler.inCooldown(1, now));
        Assertions.assertFalse(scheduler.inCooldown(1, now + 3_600_001L));
        Assertions.assertTrue(scheduler.reserve(FINAL, 2, 100, now));
    }

    @Test
    public void testExpireFreesUnreportedTask() {
        Config.final_compaction_task_timeout_second = 100;
        Config.final_compaction_cooldown_second = 50;
        CloudCompactionScheduler scheduler = new CloudCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(FINAL, 1, 100, 0));

        scheduler.expire(99_999L);
        Assertions.assertEquals(1, scheduler.inFlightCount(FINAL, 100));

        scheduler.expire(100_000L);
        Assertions.assertEquals(0, scheduler.inFlightCount(FINAL, 100));
        Assertions.assertTrue(scheduler.inCooldown(1, 100_000L));

        scheduler.expire(150_000L);
        Assertions.assertFalse(scheduler.inCooldown(1, 150_000L));
    }

    @Test
    public void testHasCompactionDebt() {
        long now = 10_000_000L;
        // Enough rowsets and no cumulative compaction for the idle period.
        Assertions.assertTrue(CloudCompactionScheduler.hasCompactionDebt(64, now - 1_800_000L, now, 64, 1_800_000L));
        // Never compacted.
        Assertions.assertTrue(CloudCompactionScheduler.hasCompactionDebt(500, 0, now, 64, 1_800_000L));
        // Compacted recently: the backend is on it.
        Assertions.assertFalse(CloudCompactionScheduler.hasCompactionDebt(500, now - 60_000L, now, 64, 1_800_000L));
        // Too few rowsets, or no stats yet.
        Assertions.assertFalse(CloudCompactionScheduler.hasCompactionDebt(63, 0, now, 64, 1_800_000L));
        Assertions.assertFalse(CloudCompactionScheduler.hasCompactionDebt(-1, 0, now, 64, 1_800_000L));
    }

    @Test
    public void testRulesHaveTheirOwnCapsAndCooldowns() {
        Config.final_compaction_max_tasks_per_be = 1;
        Config.compaction_debt_max_tasks_per_be = 2;
        Config.final_compaction_cooldown_second = 3600;
        Config.compaction_debt_cooldown_second = 60;
        CloudCompactionScheduler scheduler = new CloudCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(FINAL, 1, 100, 0));
        Assertions.assertFalse(scheduler.reserve(FINAL, 2, 100, 0));
        Assertions.assertTrue(scheduler.reserve(DEBT, 2, 100, 0));
        Assertions.assertTrue(scheduler.reserve(DEBT, 3, 100, 0));
        Assertions.assertFalse(scheduler.reserve(DEBT, 4, 100, 0));
        // A tablet in flight under one rule is not sent the other.
        Assertions.assertFalse(scheduler.reserve(DEBT, 1, 200, 0));
        Assertions.assertEquals(1, scheduler.inFlightCount(FINAL, 100));
        Assertions.assertEquals(2, scheduler.inFlightCount(DEBT, 100));

        scheduler.onTaskFinished(100, 2, new TStatus(TStatusCode.OK));
        long now = System.currentTimeMillis();
        Assertions.assertTrue(scheduler.inCooldown(2, now));
        Assertions.assertFalse(scheduler.inCooldown(2, now + 61_000L));
        Assertions.assertEquals(1, scheduler.inFlightCount(DEBT, 100));
        Assertions.assertTrue(scheduler.reserve(DEBT, 4, 100, now));
    }
}
