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

#include <aws/core/client/AWSError.h>
#include <aws/core/client/CoreErrors.h>
#include <gtest/gtest.h>

#include <set>

#include "cpp/obj_retry_strategy.h"

namespace doris {

namespace {
Aws::Client::AWSError<Aws::Client::CoreErrors> slow_down() {
    return {Aws::Client::CoreErrors::SLOW_DOWN, true};
}
} // namespace

// Without jitter the delays are DefaultRetryStrategy's: 25 ms << attempt.
TEST(S3RetryJitterTest, WithoutJitterTheDelayIsExponential) {
    S3CustomRetryStrategy strategy(10, true, [] { return false; });
    for (long attempt = 1; attempt < 10; ++attempt) {
        EXPECT_EQ(strategy.CalculateDelayBeforeNextRetry(slow_down(), attempt), 25L << attempt);
    }
    S3CustomRetryStrategy no_flag(10);
    EXPECT_EQ(no_flag.CalculateDelayBeforeNextRetry(slow_down(), 3), 200);
}

// With jitter each delay lies in [0, the exponential delay] and varies between retries.
TEST(S3RetryJitterTest, WithJitterTheDelayIsDrawnUpToTheExponentialDelay) {
    S3CustomRetryStrategy strategy(10, true, [] { return true; });
    for (long attempt = 1; attempt < 10; ++attempt) {
        std::set<long> seen;
        for (int i = 0; i < 200; ++i) {
            const long delay = strategy.CalculateDelayBeforeNextRetry(slow_down(), attempt);
            EXPECT_GE(delay, 0);
            EXPECT_LE(delay, 25L << attempt);
            seen.insert(delay);
        }
        EXPECT_GT(seen.size(), 10);
    }
    EXPECT_EQ(full_jitter_delay_ms(0), 0);
}

} // namespace doris
