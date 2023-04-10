################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.datastream.connectors import Source
from pyflink.java_gateway import get_gateway

__all__ = [
    'RateLimiterStrategy',
    'Datagen',
    'DatagenStr'
]


class RateLimiterStrategy(object):
    """
    A factory which apply rate-limiting to a source.
    """

    def __init__(self, j_rate_limiter_strategy):
        self._j_rate_limiter_strategy = j_rate_limiter_strategy

    """"
    Creates a RateLimiterStrategy that is limiting the number of records per second.
    """
    @staticmethod
    def per_second(records_per_second: float) -> 'RateLimiterStrategy':
        JRateLimiterStrategy = get_gateway().jvm.org.apache.flink. \
            api.connector.source.util.ratelimit.RateLimiterStrategy
        return RateLimiterStrategy(JRateLimiterStrategy.perSecond(records_per_second))

    """
    Creates a RateLimiterStrategy that is limiting the number of records per checkpoint.
    """
    @staticmethod
    def per_checkpoint(records_per_checkpoint: int) -> 'RateLimiterStrategy':
        JRateLimiterStrategy = get_gateway().jvm.org.apache.flink. \
            api.connector.source.util.ratelimit.RateLimiterStrategy
        return RateLimiterStrategy(JRateLimiterStrategy.perCheckpoint(records_per_checkpoint))

    """
    Creates a convenience RateLimiterStrategy that is not limiting the records rate.
    """
    @staticmethod
    def no_op() -> 'RateLimiterStrategy':
        JRateLimiterStrategy = get_gateway().jvm.org.apache.flink. \
            api.connector.source.util.ratelimit.RateLimiterStrategy
        return RateLimiterStrategy(JRateLimiterStrategy.noOp())


class Datagen(Source):
    """
    A data source that produces a sequence of numbers (int). This source is useful for testing and
    for cases that just need a stream of N events of sequence of numbers.

    The order of elements depends on the parallelism. Each sub-sequence will be produced in order.
    Consequently, if the parallelism is limited to one, this will produce one sequence in order from
    "Number: 0" to "Number: 999"

    This source has built-in support for rate limiting. The following code will produce an
    effectively unbounded (sys.maxsize from practical perspective will never be reached) stream of
    int values at the overall source rate (across all source subtasks) of 100 events per second.

    Datagen(sys.maxsize, RateLimiterStrategy.per_second(100))

    This source is always bounded. For very int sequences (for example over the entire domain of
    integer values), user may want to consider executing the application in a streaming manner,
    because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
    """

    def __init__(self, count: int, rate_limiter_strategy: RateLimiterStrategy):
        """
        Creates a new Datagen Source that produces parallel sequences covering the
        range 0 to count-1 (both boundaries are inclusive).
        """
        JDataGenerator = get_gateway().jvm.org.apache.flink.streaming.conncetor.datagen.source.python. \
            DataGenerator
        j_data_generator = JDataGenerator(count, rate_limiter_strategy._j_rate_limiter_strategy)
        super(Datagen, self).__init__(source=j_data_generator)


class DatagenStr(Source):
    """
    A data source that produces random string with a given length. This source is useful for testing
    and for cases that just need a stream of N events of random string kind.

    This source has built-in support for rate limiting. The following code will produce an
    effectively unbounded (sys.maxsize from practical perspective will never be reached) stream of
    random string at the overall source rate (across all source subtasks) of 100 events per second.

    DatagenStr(sys.maxsize, 10, RateLimiterStrategy.per_second(100))

    This source is always bounded. For very int sequences (for example over the entire domain of
    integer values), user may want to consider executing the application in a streaming manner,
    because, despite the fact that the produced stream is bounded, the end bound is pretty far away.
    """

    def __init__(self, count: int, length: int, rate_limiter_strategy: RateLimiterStrategy):
        """
        Creates a new DatagenStr Source that produces parallel random string with totally count
        amount.
        """
        JDataGeneratorStr = get_gateway().jvm.org.apache.flink.streaming.conncetor.datagen.source.python. \
            DataGeneratorStr
        j_data_generator_str = JDataGeneratorStr(count, length, rate_limiter_strategy._j_rate_limiter_strategy)
        super(DatagenStr, self).__init__(source=j_data_generator_str)

