/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.time.Duration;

/**
 * A {@link Trigger} that continuously fires based on a given time interval as measured by the clock
 * of the machine on which the job is running.
 *
 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
 */
@PublicEvolving
public class ContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long interval;

    /** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);

    private ContinuousProcessingTimeTrigger(long interval) {
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx)
            throws Exception {
        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        timestamp = ctx.getCurrentProcessingTime();

        if (fireTimestampState.get() == null) {
            registerNextFireTimestamp(
                    timestamp - (timestamp % interval), window, ctx, fireTimestampState);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx)
            throws Exception {
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE;
        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        if (fireTimestampState.get().equals(time)) {
            fireTimestampState.clear();
            registerNextFireTimestamp(time, window, ctx, fireTimestampState);
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        // State could be merged into new window.
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteProcessingTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        // States for old windows will lose after the call.
        ctx.mergePartitionedState(stateDesc);

        // Register timer for this new window.
        Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
        if (nextFireTimestamp != null) {
            ctx.registerProcessingTimeTimer(nextFireTimestamp);
        }
    }

    @VisibleForTesting
    public long getInterval() {
        return interval;
    }

    @Override
    public String toString() {
        return "ContinuousProcessingTimeTrigger(" + interval + ")";
    }

    /**
     * Creates a trigger that continuously fires based on the given interval.
     *
     * @param interval The time interval at which to fire.
     * @param <W> The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <W extends Window> ContinuousProcessingTimeTrigger<W> of(Duration interval) {
        return new ContinuousProcessingTimeTrigger<>(interval.toMillis());
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

    private void registerNextFireTimestamp(
            long time, W window, TriggerContext ctx, ReducingState<Long> fireTimestampState)
            throws Exception {
        long nextFireTimestamp = Math.min(time + interval, window.maxTimestamp());
        fireTimestampState.add(nextFireTimestamp);
        ctx.registerProcessingTimeTimer(nextFireTimestamp);
    }
}
