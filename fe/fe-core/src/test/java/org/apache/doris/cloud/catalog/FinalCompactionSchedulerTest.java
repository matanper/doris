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

public class FinalCompactionSchedulerTest {
    private static final long MB = 1024L * 1024L;

    private int oldMaxTasksPerBe;
    private long oldTimeoutSecond;
    private long oldCooldownSecond;

    @BeforeEach
    public void setUp() {
        oldMaxTasksPerBe = Config.final_compaction_max_tasks_per_be;
        oldTimeoutSecond = Config.final_compaction_task_timeout_second;
        oldCooldownSecond = Config.final_compaction_cooldown_second;
    }

    @AfterEach
    public void tearDown() {
        Config.final_compaction_max_tasks_per_be = oldMaxTasksPerBe;
        Config.final_compaction_task_timeout_second = oldTimeoutSecond;
        Config.final_compaction_cooldown_second = oldCooldownSecond;
    }

    @Test
    public void testTargetSegmentBytesFollowsTablePolicy() {
        Assertions.assertEquals(2048 * MB,
                FinalCompactionScheduler.targetSegmentBytes("time_series", 1024L, 1024));
        Assertions.assertEquals(1024 * MB,
                FinalCompactionScheduler.targetSegmentBytes("size_based", 1024L, 1024));
        Assertions.assertEquals(1024 * MB,
                FinalCompactionScheduler.targetSegmentBytes("time_series", null, 1024));
        Assertions.assertEquals(512 * MB,
                FinalCompactionScheduler.targetSegmentBytes(null, null, 512));
    }

    @Test
    public void testIdealSegments() {
        Assertions.assertEquals(1, FinalCompactionScheduler.idealSegments(0, 1024 * MB));
        Assertions.assertEquals(1, FinalCompactionScheduler.idealSegments(1024 * MB, 1024 * MB));
        Assertions.assertEquals(2, FinalCompactionScheduler.idealSegments(1024 * MB + 1, 1024 * MB));
        Assertions.assertEquals(7, FinalCompactionScheduler.idealSegments(13_630 * MB, 2048 * MB));
    }

    @Test
    public void testIsFragmented() {
        long target = 2048 * MB;
        // The dc1 tablet: 275 segments, 13.6 GB of segment data, 7 segments ideal.
        Assertions.assertTrue(FinalCompactionScheduler.isFragmented(275, 13_630 * MB, target, 4.0, 16));
        Assertions.assertFalse(FinalCompactionScheduler.isFragmented(27, 13_630 * MB, target, 4.0, 16));
        Assertions.assertTrue(FinalCompactionScheduler.isFragmented(28, 13_630 * MB, target, 4.0, 16));
        // Small tablets need the minimum segment count too.
        Assertions.assertFalse(FinalCompactionScheduler.isFragmented(15, 10 * MB, target, 4.0, 16));
        Assertions.assertTrue(FinalCompactionScheduler.isFragmented(16, 10 * MB, target, 4.0, 16));
        // No stats yet.
        Assertions.assertFalse(FinalCompactionScheduler.isFragmented(-1, 0, target, 4.0, 16));
    }

    @Test
    public void testIsQuiet() {
        long now = 10_000_000L;
        Assertions.assertTrue(FinalCompactionScheduler.isQuiet(now - 7_200_000L, now, 7_200_000L));
        Assertions.assertFalse(FinalCompactionScheduler.isQuiet(now - 7_199_999L, now, 7_200_000L));
        // Unknown version time.
        Assertions.assertFalse(FinalCompactionScheduler.isQuiet(0, now, 7_200_000L));
    }

    @Test
    public void testReserveCapsTasksPerBackend() {
        Config.final_compaction_max_tasks_per_be = 2;
        FinalCompactionScheduler scheduler = new FinalCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(1, 100, 0));
        Assertions.assertFalse(scheduler.reserve(1, 100, 0));
        Assertions.assertTrue(scheduler.reserve(2, 100, 0));
        Assertions.assertFalse(scheduler.reserve(3, 100, 0));
        Assertions.assertTrue(scheduler.reserve(3, 200, 0));
        Assertions.assertEquals(2, scheduler.inFlightCount(100));
        Assertions.assertEquals(1, scheduler.inFlightCount(200));
    }

    @Test
    public void testReportFreesSlotAndStartsCooldown() {
        Config.final_compaction_max_tasks_per_be = 1;
        Config.final_compaction_cooldown_second = 3600;
        FinalCompactionScheduler scheduler = new FinalCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(1, 100, 0));

        // A report from another backend is not about this dispatch.
        scheduler.onTaskFinished(200, 1, new TStatus(TStatusCode.OK));
        Assertions.assertEquals(1, scheduler.inFlightCount(100));

        scheduler.onTaskFinished(100, 1, new TStatus(TStatusCode.CANCELLED));
        Assertions.assertEquals(0, scheduler.inFlightCount(100));
        long now = System.currentTimeMillis();
        Assertions.assertTrue(scheduler.inCooldown(1, now));
        Assertions.assertFalse(scheduler.inCooldown(1, now + 3_600_001L));
        Assertions.assertTrue(scheduler.reserve(2, 100, now));
    }

    @Test
    public void testExpireFreesUnreportedTask() {
        Config.final_compaction_task_timeout_second = 100;
        Config.final_compaction_cooldown_second = 50;
        FinalCompactionScheduler scheduler = new FinalCompactionScheduler();
        Assertions.assertTrue(scheduler.reserve(1, 100, 0));

        scheduler.expire(99_999L);
        Assertions.assertEquals(1, scheduler.inFlightCount(100));

        scheduler.expire(100_000L);
        Assertions.assertEquals(0, scheduler.inFlightCount(100));
        Assertions.assertTrue(scheduler.inCooldown(1, 100_000L));

        scheduler.expire(150_000L);
        Assertions.assertFalse(scheduler.inCooldown(1, 150_000L));
    }
}
