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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CloudTabletOwnershipPublisherTest {
    private static final String GROUP_A = "group_a";
    private static final String GROUP_B = "group_b";

    private MockedStatic<Env> envMockedStatic;
    private final Map<Long, String> backendToCluster = new HashMap<>();
    private final List<String> clusters = Arrays.asList(GROUP_A, GROUP_B);

    @BeforeEach
    public void setUp() {
        ColocateTableIndex colocateIndex = Mockito.mock(ColocateTableIndex.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateIndex);
        backendToCluster.put(1L, GROUP_A);
        backendToCluster.put(2L, GROUP_A);
        backendToCluster.put(3L, GROUP_B);
    }

    @AfterEach
    public void tearDown() {
        envMockedStatic.close();
    }

    private static CloudReplica replica(long id) {
        return new CloudReplica(id, -1L, Replica.ReplicaState.NORMAL, 1, 0, 1L, 2L, 3L, 4L, 0);
    }

    @Test
    public void testPrimaryBackendOwnsTabletInEachGroup() {
        Map<Long, List<Long>> owned = new HashMap<>();
        CloudReplica onBoth = replica(100);
        onBoth.updateClusterToPrimaryBe(GROUP_A, 1L);
        onBoth.updateClusterToPrimaryBe(GROUP_B, 3L);
        CloudTabletOwnershipPublisher.addOwners(owned, 10L, onBoth, clusters, backendToCluster);

        CloudReplica onA = replica(101);
        onA.updateClusterToPrimaryBe(GROUP_A, 2L);
        CloudTabletOwnershipPublisher.addOwners(owned, 11L, onA, clusters, backendToCluster);

        Assertions.assertEquals(Collections.singletonList(10L), owned.get(1L));
        Assertions.assertEquals(Collections.singletonList(11L), owned.get(2L));
        Assertions.assertEquals(Collections.singletonList(10L), owned.get(3L));
    }

    @Test
    public void testBackendServingForItsPrimaryDoesNotOwnTablet() {
        Map<Long, List<Long>> owned = new HashMap<>();
        CloudReplica covered = replica(100);
        covered.updateClusterToPrimaryBe(GROUP_A, 1L);
        covered.updateClusterToSecondaryBe(GROUP_A, 2L);
        CloudTabletOwnershipPublisher.addOwners(owned, 10L, covered, clusters, backendToCluster);

        Assertions.assertEquals(Collections.singletonList(10L), owned.get(1L));
        Assertions.assertNull(owned.get(2L));
    }

    @Test
    public void testUnassignedOrUnknownPrimaryHasNoOwner() {
        Map<Long, List<Long>> owned = new HashMap<>();
        CloudTabletOwnershipPublisher.addOwners(owned, 10L, replica(100), clusters, backendToCluster);
        // The primary recorded for a group is a backend no longer in that group.
        CloudReplica moved = replica(101);
        moved.updateClusterToPrimaryBe(GROUP_B, 1L);
        CloudTabletOwnershipPublisher.addOwners(owned, 11L, moved, clusters, backendToCluster);

        Assertions.assertTrue(owned.isEmpty());
    }
}
