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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A lambda in a nested loop join's other condition must stay whole: its body may reference the
 * lambda arguments, which no join child outputs.
 */
public class ProjectOtherJoinConditionLambdaTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("lambda_nlj");
        useDatabase("lambda_nlj");
        createTables(
                "create table t1 (k int not null, s varchar(64), v variant null) duplicate key(k) "
                        + "distributed by hash(k) buckets 3 properties('replication_num' = '1')",
                "create table t2 (k int not null, s varchar(64), v variant null) duplicate key(k) "
                        + "distributed by hash(k) buckets 3 properties('replication_num' = '1')");
    }

    private void assertPlans(String sql) throws Exception {
        String plan = getSQLPlanOrErrorMsg("explain " + sql);
        Assertions.assertTrue(plan.contains("NESTED LOOP JOIN"), plan);
        Assertions.assertFalse(plan.contains("errCode"), plan);
    }

    @Test
    public void testLambdaCapturingTheOtherSideStaysInTheJoinCondition() throws Exception {
        assertPlans("select count(*) from t1 where array_match_any(c -> t1.s = c, "
                + "(select collect_list(s) from t2))");
        assertPlans("select count(*) from t1 where not array_match_any(c -> t1.s = c, "
                + "(select collect_list(s) from t2))");
        assertPlans("select count(*) from t1 where array_match_any(c -> concat(t1.s, cast(t1.k as string)) = c, "
                + "(select collect_list(s) from t2))");
        assertPlans("select t1.s from t1 where array_match_any(c -> cast(t1.v['ip'] as string) = c, "
                + "(select collect_list(cast(v['ip'] as string)) from t2))");
        assertPlans("select count(*) from t1 where array_match_any(c -> c = 'x', "
                + "(select collect_list(s) from t2))");
    }
}
