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
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The specific implementation of {@link ResourceAllocationStrategy} for OLAP scenarios.
 *
 * <p>Note: This strategy do not support redundant resources and fine-grained resource management.
 */
public class OlapResourceAllocationStrategy implements ResourceAllocationStrategy {

    private final ResourceProfile defaultSlotResourceProfile;
    private final ResourceProfile totalResourceProfile;
    private final int numSlotsPerWorker;
    private final ResourceMatchingStrategy availableResourceMatchingStrategy;

    /**
     * Always use any matching strategy for pending resources to use as less pending workers as
     * possible, so that the rest can be canceled
     */
    private final ResourceMatchingStrategy pendingResourceMatchingStrategy =
            AnyMatchingResourceMatchingStrategy.INSTANCE;

    private final Time taskManagerTimeout;

    private final int minSlotNum;

    public OlapResourceAllocationStrategy(
            ResourceProfile totalResourceProfile,
            int numSlotsPerWorker,
            boolean evenlySpreadOutSlots,
            Time taskManagerTimeout,
            int minSlotNum) {
        this.totalResourceProfile = totalResourceProfile;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        totalResourceProfile, numSlotsPerWorker);
        this.availableResourceMatchingStrategy =
                evenlySpreadOutSlots
                        ? LeastUtilizationResourceMatchingStrategy.INSTANCE
                        : AnyMatchingResourceMatchingStrategy.INSTANCE;
        this.taskManagerTimeout = taskManagerTimeout;
        this.minSlotNum = minSlotNum;
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        final ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();

        final List<InternalResourceInfo> registeredResources =
                getAvailableResources(
                        taskManagerResourceInfoProvider, resultBuilder, blockedTaskManagerChecker);
        final List<InternalResourceInfo> pendingResources =
                getPendingResources(taskManagerResourceInfoProvider, resultBuilder);

        int totalCurrentSlots =
                Stream.concat(registeredResources.stream(), pendingResources.stream())
                        .mapToInt(internalResourceInfo -> internalResourceInfo.totalSlots)
                        .sum();

        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();
            final int requiredSlots =
                    resourceRequirements.getValue().stream()
                            .mapToInt(ResourceRequirement::getNumberOfRequiredSlots)
                            .sum();

            final int unfulfilledJobSlots =
                    availableResourceMatchingStrategy.tryFulfilledRequirementWithResource(
                            registeredResources, requiredSlots, jobId);

            if (unfulfilledJobSlots > 0) {
                totalCurrentSlots =
                        tryFulfillRequirementsForJobWithPendingResources(
                                jobId,
                                unfulfilledJobSlots,
                                pendingResources,
                                totalCurrentSlots,
                                resultBuilder);
            }
        }

        // Unlike tryFulfillRequirementsForJobWithPendingResources, which updates pendingResources
        // to the latest state after a new PendingTaskManager is created,
        // tryFulFillRequiredResources will not update pendingResources even after new
        // PendingTaskManagers are created.
        // This is because the pendingResources are no longer needed afterwards.
        tryFulFillRequiredResources(totalCurrentSlots, resultBuilder);
        return resultBuilder.build();
    }

    @Override
    public ResourceReconcileResult tryReconcileClusterResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        ResourceReconcileResult.Builder builder = ResourceReconcileResult.builder();

        List<TaskManagerInfo> taskManagersIdleTimeout = new ArrayList<>();
        List<TaskManagerInfo> taskManagersNonTimeout = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        taskManagerResourceInfoProvider
                .getRegisteredTaskManagers()
                .forEach(
                        taskManagerInfo -> {
                            if (taskManagerInfo.isIdle()
                                    && currentTime - taskManagerInfo.getIdleSince()
                                            >= taskManagerTimeout.toMilliseconds()) {
                                taskManagersIdleTimeout.add(taskManagerInfo);
                            } else {
                                taskManagersNonTimeout.add(taskManagerInfo);
                            }
                        });

        List<PendingTaskManager> pendingTaskManagersNonUse = new ArrayList<>();
        List<PendingTaskManager> pendingTaskManagersInuse = new ArrayList<>();
        taskManagerResourceInfoProvider
                .getPendingTaskManagers()
                .forEach(
                        pendingTaskManager -> {
                            if (pendingTaskManager.getPendingSlotAllocationRecords().isEmpty()) {
                                pendingTaskManagersNonUse.add(pendingTaskManager);
                            } else {
                                pendingTaskManagersInuse.add(pendingTaskManager);
                            }
                        });

        int slotsInTotal = 0;
        boolean resourceFulfilled = false;

        int slotsInTotalOfNonIdle = getTotalResourceOfTaskManagers(taskManagersNonTimeout);

        slotsInTotal = slotsInTotal + slotsInTotalOfNonIdle;

        if (isMinRequiredResourcesFulfilled(slotsInTotal)) {
            resourceFulfilled = true;
        } else {
            int slotsInTotalOfNonIdlePendingTaskManager =
                    getTotalResourceOfPendingTaskManagers(pendingTaskManagersInuse);

            slotsInTotal = slotsInTotal + slotsInTotalOfNonIdlePendingTaskManager;
        }

        // try reserve or release unused (pending) task managers
        for (TaskManagerInfo taskManagerInfo : taskManagersIdleTimeout) {
            if (resourceFulfilled || isMinRequiredResourcesFulfilled(slotsInTotal)) {
                resourceFulfilled = true;
                builder.addTaskManagerToRelease(taskManagerInfo);
            } else {
                slotsInTotal = slotsInTotal + taskManagerInfo.getDefaultNumSlots();
            }
        }
        for (PendingTaskManager pendingTaskManager : pendingTaskManagersNonUse) {
            if (resourceFulfilled || isMinRequiredResourcesFulfilled(slotsInTotal)) {
                resourceFulfilled = true;
                builder.addPendingTaskManagerToRelease(pendingTaskManager);
            } else {
                slotsInTotal = slotsInTotal + pendingTaskManager.getNumSlots();
            }
        }

        if (!resourceFulfilled) {
            // fulfill required resources
            tryFulFillRequiredResourcesWithAction(
                    slotsInTotal, builder::addPendingTaskManagerToAllocate);
        }

        return builder.build();
    }

    private static List<InternalResourceInfo> getAvailableResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            ResourceAllocationResult.Builder resultBuilder,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        return taskManagerResourceInfoProvider.getRegisteredTaskManagers().stream()
                .filter(
                        taskManager ->
                                !blockedTaskManagerChecker.isBlockedTaskManager(
                                        taskManager.getTaskExecutorConnection().getResourceID()))
                .map(
                        taskManager ->
                                new InternalResourceInfo(
                                        taskManager.getDefaultNumSlots(),
                                        taskManager.getDefaultNumSlots()
                                                - taskManager.getAllocatedSlots().size(),
                                        jobId ->
                                                resultBuilder.addAllocationOnRegisteredResource(
                                                        jobId,
                                                        taskManager.getInstanceId(),
                                                        taskManager
                                                                .getDefaultSlotResourceProfile())))
                .collect(Collectors.toList());
    }

    private static List<InternalResourceInfo> getPendingResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            ResourceAllocationResult.Builder resultBuilder) {
        return taskManagerResourceInfoProvider.getPendingTaskManagers().stream()
                .map(
                        pendingTaskManager ->
                                new InternalResourceInfo(
                                        pendingTaskManager.getNumSlots(),
                                        pendingTaskManager.getNumSlots(),
                                        jobId ->
                                                resultBuilder.addAllocationOnPendingResource(
                                                        jobId,
                                                        pendingTaskManager
                                                                .getPendingTaskManagerId(),
                                                        pendingTaskManager
                                                                .getDefaultSlotResourceProfile())))
                .collect(Collectors.toList());
    }

    private int tryFulfillRequirementsForJobWithPendingResources(
            JobID jobId,
            int missingSlots,
            List<InternalResourceInfo> availableResources,
            int totalCurrentSlots,
            ResourceAllocationResult.Builder resultBuilder) {
        int numUnfulfilled =
                pendingResourceMatchingStrategy.tryFulfilledRequirementWithResource(
                        availableResources, missingSlots, jobId);

        while (numUnfulfilled > 0) {
            // Circularly add new pending task manager
            final PendingTaskManager newPendingTaskManager =
                    new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
            resultBuilder.addPendingTaskManagerAllocate(newPendingTaskManager);
            totalCurrentSlots += numSlotsPerWorker;
            int remainSlots = numSlotsPerWorker;
            while (numUnfulfilled > 0 && remainSlots > 0) {
                numUnfulfilled--;
                resultBuilder.addAllocationOnPendingResource(
                        jobId,
                        newPendingTaskManager.getPendingTaskManagerId(),
                        defaultSlotResourceProfile);
                remainSlots--;
            }
            if (remainSlots > 0) {
                availableResources.add(
                        new InternalResourceInfo(
                                numSlotsPerWorker,
                                remainSlots,
                                jobID ->
                                        resultBuilder.addAllocationOnPendingResource(
                                                jobID,
                                                newPendingTaskManager.getPendingTaskManagerId(),
                                                defaultSlotResourceProfile)));
            }
        }
        return totalCurrentSlots;
    }

    private boolean isMinRequiredResourcesFulfilled(int slotsInTotal) {
        return slotsInTotal >= minSlotNum;
    }

    private void tryFulFillRequiredResources(
            int totalCurrentSlots, ResourceAllocationResult.Builder resultBuilder) {

        tryFulFillRequiredResourcesWithAction(
                totalCurrentSlots, resultBuilder::addPendingTaskManagerAllocate);
    }

    private void tryFulFillRequiredResourcesWithAction(
            int slotsInTotal, Consumer<? super PendingTaskManager> fulfillAction) {
        while (!isMinRequiredResourcesFulfilled(slotsInTotal)) {
            PendingTaskManager pendingTaskManager =
                    new PendingTaskManager(totalResourceProfile, numSlotsPerWorker);
            fulfillAction.accept(pendingTaskManager);
            slotsInTotal += numSlotsPerWorker;
        }
    }

    private int getTotalResourceOfTaskManagers(List<TaskManagerInfo> taskManagers) {
        return taskManagers.size() * numSlotsPerWorker;
    }

    private int getTotalResourceOfPendingTaskManagers(
            List<PendingTaskManager> pendingTaskManagers) {
        return pendingTaskManagers.size() * numSlotsPerWorker;
    }

    private static class InternalResourceInfo {
        private final Consumer<JobID> allocationConsumer;
        private final int totalSlots;
        private int availableSlots;
        private double utilization;

        InternalResourceInfo(
                int totalSlots, int availableSlots, Consumer<JobID> allocationConsumer) {
            Preconditions.checkState(availableSlots >= 0);
            Preconditions.checkState(totalSlots >= availableSlots);
            this.totalSlots = totalSlots;
            this.availableSlots = availableSlots;
            this.allocationConsumer = allocationConsumer;
            this.utilization = updateUtilization();
        }

        boolean tryAllocateSlotForJob(JobID jobId) {
            if (availableSlots > 0) {
                availableSlots -= 1;
                allocationConsumer.accept(jobId);
                utilization = updateUtilization();
                return true;
            } else {
                return false;
            }
        }

        private double updateUtilization() {
            double utilization = (totalSlots - availableSlots) * 1.0 / totalSlots;
            return Math.max(utilization, 0);
        }
    }

    private interface ResourceMatchingStrategy {

        int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources, int numUnfulfilled, JobID jobId);
    }

    private enum AnyMatchingResourceMatchingStrategy implements ResourceMatchingStrategy {
        INSTANCE;

        @Override
        public int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources, int numUnfulfilled, JobID jobId) {
            final Iterator<InternalResourceInfo> internalResourceInfoItr =
                    internalResources.iterator();
            while (numUnfulfilled > 0 && internalResourceInfoItr.hasNext()) {
                final InternalResourceInfo currentTaskManager = internalResourceInfoItr.next();
                while (numUnfulfilled > 0 && currentTaskManager.tryAllocateSlotForJob(jobId)) {
                    numUnfulfilled--;
                }
                if (currentTaskManager.availableSlots == 0) {
                    internalResourceInfoItr.remove();
                }
            }
            return numUnfulfilled;
        }
    }

    private enum LeastUtilizationResourceMatchingStrategy implements ResourceMatchingStrategy {
        INSTANCE;

        @Override
        public int tryFulfilledRequirementWithResource(
                List<InternalResourceInfo> internalResources, int numUnfulfilled, JobID jobId) {
            if (internalResources.isEmpty()) {
                return numUnfulfilled;
            }

            Queue<InternalResourceInfo> resourceInfoInUtilizationOrder =
                    new PriorityQueue<>(
                            internalResources.size(),
                            Comparator.comparingDouble(i -> i.utilization));
            resourceInfoInUtilizationOrder.addAll(internalResources);

            while (numUnfulfilled > 0 && !resourceInfoInUtilizationOrder.isEmpty()) {
                final InternalResourceInfo currentTaskManager =
                        resourceInfoInUtilizationOrder.poll();

                if (currentTaskManager.tryAllocateSlotForJob(jobId)) {
                    numUnfulfilled--;

                    // ignore non resource task managers to reduce the overhead of insert.
                    if (currentTaskManager.availableSlots != 0) {
                        resourceInfoInUtilizationOrder.add(currentTaskManager);
                    }
                }
            }
            return numUnfulfilled;
        }
    }
}
