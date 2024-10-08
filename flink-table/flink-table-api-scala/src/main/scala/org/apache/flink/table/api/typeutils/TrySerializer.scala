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
package org.apache.flink.table.api.typeutils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.serialization.SerializerConfig
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.util.{Failure, Success, Try}

/** Serializer for [[scala.util.Try]]. */
@Internal
@SerialVersionUID(-3052182891252564491L)
class TrySerializer[A](
    private val elemSerializer: TypeSerializer[A],
    private val throwableSerializer: TypeSerializer[Throwable])
  extends TypeSerializer[Try[A]] {

  private[typeutils] def this(
      elemSerializer: TypeSerializer[A],
      serializerConfig: SerializerConfig) = {
    this(
      elemSerializer,
      new KryoSerializer[Throwable](classOf[Throwable], serializerConfig)
    )
  }

  override def duplicate: TrySerializer[A] = new TrySerializer[A](
    elemSerializer.duplicate(),
    throwableSerializer.duplicate()
  )

  override def createInstance: Try[A] = {
    Failure(new RuntimeException("Empty Failure"))
  }

  override def isImmutableType: Boolean = elemSerializer == null || elemSerializer.isImmutableType

  override def getLength: Int = -1

  override def copy(from: Try[A]): Try[A] = from match {
    case Success(a) => Success(elemSerializer.copy(a))
    case Failure(t) => Failure(throwableSerializer.copy(t))
  }

  override def copy(from: Try[A], reuse: Try[A]): Try[A] = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val isSuccess = source.readBoolean()
    target.writeBoolean(isSuccess)
    if (isSuccess) {
      elemSerializer.copy(source, target)
    } else {
      throwableSerializer.copy(source, target)
    }
  }

  override def serialize(either: Try[A], target: DataOutputView): Unit = either match {
    case Success(a) =>
      target.writeBoolean(true)
      elemSerializer.serialize(a, target)
    case Failure(t) =>
      target.writeBoolean(false)
      throwableSerializer.serialize(t, target)
  }

  override def deserialize(source: DataInputView): Try[A] = {
    val isSuccess = source.readBoolean()
    if (isSuccess) {
      Success(elemSerializer.deserialize(source))
    } else {
      Failure(throwableSerializer.deserialize(source))
    }
  }

  override def deserialize(reuse: Try[A], source: DataInputView): Try[A] = deserialize(source)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: TrySerializer[_] =>
        elemSerializer.equals(other.elemSerializer)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    31 * elemSerializer.hashCode() + throwableSerializer.hashCode()
  }

  // --------------------------------------------------------------------------------------------
  // Serializer configuration snapshotting & compatibility
  // --------------------------------------------------------------------------------------------

  private[typeutils] def getElementSerializer: TypeSerializer[A] = elemSerializer
  private[typeutils] def getThrowableSerializer: TypeSerializer[Throwable] = throwableSerializer

  override def snapshotConfiguration(): ScalaTrySerializerSnapshot[A] = {
    new ScalaTrySerializerSnapshot[A](this)
  }
}
