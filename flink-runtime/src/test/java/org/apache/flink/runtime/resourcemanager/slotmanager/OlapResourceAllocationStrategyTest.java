/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link OlapResourceAllocationStrategy}. */
class OlapResourceAllocationStrategyTest {
    private static final ResourceProfile DEFAULT_SLOT_RESOURCE =
            ResourceProfile.fromResources(1, 100);
    private static final int NUM_OF_SLOTS = 5;
    private static final OlapResourceAllocationStrategy ANY_MATCHING_STRATEGY =
            createStrategy(false);

    private static final OlapResourceAllocationStrategy EVENLY_STRATEGY = createStrategy(true);

    @Test
    void testFulfillRequirementWithRegisteredResources() {
        final TaskManagerInfo taskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(() -> Collections.singleton(taskManager))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 4));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
        assertThat(
                        result.getAllocationsOnRegisteredResources()
                                .get(jobId)
                                .get(taskManager.getInstanceId())
                                .getResourceCount(DEFAULT_SLOT_RESOURCE))
                .isEqualTo(4);
    }

    @Test
    void testFulfillRequirementWithRegisteredResourcesEvenly() {
        final TaskManagerInfo taskManager1 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerInfo taskManager2 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerInfo taskManager3 =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE.multiply(10),
                        DEFAULT_SLOT_RESOURCE);

        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManager1, taskManager2, taskManager3))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 9));

        final ResourceAllocationResult result =
                EVENLY_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();

        assertThat(result.getAllocationsOnRegisteredResources().get(jobId).values())
                .allSatisfy(
                        resourceCounter ->
                                assertThat(resourceCounter.getTotalResourceCount()).isEqualTo(3));
    }

    @Test
    void testExcessPendingResourcesCouldReleaseEvenly() {
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () ->
                                        Arrays.asList(
                                                new PendingTaskManager(
                                                        DEFAULT_SLOT_RESOURCE.multiply(2), 2),
                                                new PendingTaskManager(
                                                        DEFAULT_SLOT_RESOURCE.multiply(2), 2)))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2));

        final ResourceAllocationResult result =
                EVENLY_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);

        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).isEmpty();
        assertThat(result.getAllocationsOnPendingResources()).hasSize(1);
    }

    @Test
    void testFulfillRequirementWithPendingResources() {
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 6));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);
        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnRegisteredResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(1);
        final PendingTaskManagerId newAllocated =
                result.getPendingTaskManagersToAllocate().get(0).getPendingTaskManagerId();
        ResourceCounter allFulfilledRequirements = ResourceCounter.empty();
        for (Map.Entry<ResourceProfile, Integer> resourceWithCount :
                result.getAllocationsOnPendingResources()
                        .get(pendingTaskManager.getPendingTaskManagerId())
                        .get(jobId)
                        .getResourcesWithCount()) {
            allFulfilledRequirements =
                    allFulfilledRequirements.add(
                            resourceWithCount.getKey(), resourceWithCount.getValue());
        }
        for (Map.Entry<ResourceProfile, Integer> resourceWithCount :
                result.getAllocationsOnPendingResources()
                        .get(newAllocated)
                        .get(jobId)
                        .getResourcesWithCount()) {
            allFulfilledRequirements =
                    allFulfilledRequirements.add(
                            resourceWithCount.getKey(), resourceWithCount.getValue());
        }

        assertThat(allFulfilledRequirements.getResourceCount(DEFAULT_SLOT_RESOURCE)).isEqualTo(6);
    }

    /** Tests that blocked task manager cannot fulfill requirements. */
    @Test
    void testBlockedTaskManagerCannotFulfillRequirements() {
        final TaskManagerInfo registeredTaskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = new ArrayList<>();
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Collections.singleton(registeredTaskManager))
                        .build();
        requirements.add(ResourceRequirement.create(ResourceProfile.UNKNOWN, 2 * NUM_OF_SLOTS));

        final ResourceAllocationResult result =
                ANY_MATCHING_STRATEGY.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        registeredTaskManager.getTaskExecutorConnection().getResourceID()::equals);

        assertThat(result.getUnfulfillableJobs()).isEmpty();
        assertThat(result.getAllocationsOnRegisteredResources()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate()).hasSize(2);
    }

    @Test
    void testIdleTaskManagerShouldBeReleased() {
        final TestingTaskManagerInfo registeredTaskManager =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Collections.singleton(registeredTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getTaskManagersToRelease()).isEmpty();

        registeredTaskManager.setIdleSince(System.currentTimeMillis() - 10);

        result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);
        assertThat(result.getTaskManagersToRelease()).containsExactly(registeredTaskManager);
    }

    @Test
    void testIdlePendingTaskManagerShouldBeReleased() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE, 1);
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).containsExactly(pendingTaskManager);
    }

    @Test
    void testUsedPendingTaskManagerShouldNotBeReleased() {
        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE, 1);
        pendingTaskManager.replaceAllPendingAllocations(
                Collections.singletonMap(
                        new JobID(), ResourceCounter.withResource(DEFAULT_SLOT_RESOURCE, 1)));
        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singleton(pendingTaskManager))
                        .build();

        ResourceReconcileResult result =
                ANY_MATCHING_STRATEGY.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
    }

    @Test
    void testMinRequiredCPULimitInTryReconcile() {
        int minRequiredSlot = NUM_OF_SLOTS * 4 + 1;

        testMinResourceLimitInReconcile(minRequiredSlot, 2);
        testMinResourceLimitInReconcileWithNoResource(minRequiredSlot, 5);
    }

    @Test
    void testMinRequiredCPULimitInTryFulfill() {
        int minRequiredSlot = NUM_OF_SLOTS * 4 + 1;

        testMinRequiredResourceLimitInFulfillRequirements(minRequiredSlot, 4);
    }

    void testMinResourceLimitInReconcile(int minRequiredSlots, int pendingTaskManagersToAllocate) {

        final TaskManagerInfo taskManagerInUse =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);

        final TestingTaskManagerInfo taskManagerIdle =
                new TestingTaskManagerInfo(
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                        DEFAULT_SLOT_RESOURCE);
        taskManagerIdle.setIdleSince(System.currentTimeMillis() - 10);

        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(
                                () -> Arrays.asList(taskManagerInUse, taskManagerIdle))
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManager))
                        .build();

        OlapResourceAllocationStrategy strategy = createStrategy(minRequiredSlots);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
        assertThat(result.getTaskManagersToRelease()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    void testMinResourceLimitInReconcileWithNoResource(
            int minRequiredSlot, int pendingTaskManagersToAllocate) {

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(Arrays::asList)
                        .setPendingTaskManagersSupplier(Arrays::asList)
                        .build();

        OlapResourceAllocationStrategy strategy = createStrategy(minRequiredSlot);
        ResourceReconcileResult result =
                strategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);

        assertThat(result.getPendingTaskManagersToRelease()).isEmpty();
        assertThat(result.getTaskManagersToRelease()).isEmpty();
        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    void testMinRequiredResourceLimitInFulfillRequirements(
            int minRequiredSlot, int pendingTaskManagersToAllocate) {

        final JobID jobId = new JobID();
        final List<ResourceRequirement> requirements = Collections.emptyList();

        final PendingTaskManager pendingTaskManager =
                new PendingTaskManager(DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS), NUM_OF_SLOTS);

        final TaskManagerResourceInfoProvider taskManagerResourceInfoProvider =
                TestingTaskManagerResourceInfoProvider.newBuilder()
                        .setRegisteredTaskManagersSupplier(Arrays::asList)
                        .setPendingTaskManagersSupplier(
                                () -> Collections.singletonList(pendingTaskManager))
                        .build();

        OlapResourceAllocationStrategy strategy = createStrategy(minRequiredSlot);
        ResourceAllocationResult result =
                strategy.tryFulfillRequirements(
                        Collections.singletonMap(jobId, requirements),
                        taskManagerResourceInfoProvider,
                        resourceID -> false);

        assertThat(result.getPendingTaskManagersToAllocate())
                .hasSize(pendingTaskManagersToAllocate);
    }

    private static OlapResourceAllocationStrategy createStrategy(boolean evenlySpreadOutSlots) {
        return new OlapResourceAllocationStrategy(
                DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                NUM_OF_SLOTS,
                evenlySpreadOutSlots,
                Time.milliseconds(0),
                0);
    }

    private static OlapResourceAllocationStrategy createStrategy(int minSlotsNum) {
        return new OlapResourceAllocationStrategy(
                DEFAULT_SLOT_RESOURCE.multiply(NUM_OF_SLOTS),
                NUM_OF_SLOTS,
                false,
                Time.milliseconds(0),
                minSlotsNum);
    }
}
