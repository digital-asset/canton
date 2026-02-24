// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.service

import io.grpc.stub.ServerCallStreamObserver

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.Promise

class ManualFlowControlServerCallStreamObserver[T](numExpected: Int)
    extends ServerCallStreamObserver[T]
    with RecordStreamObserverItems[T] {

  val promise = Promise[Seq[T]]()
  val counter = new AtomicInteger(numExpected)

  @volatile var isCancelled_ = false
  override def isCancelled: Boolean = isCancelled_

  val onCancelHandlerRef = new AtomicReference[Option[Runnable]](None)
  override def setOnCancelHandler(onCancelHandler: Runnable): Unit =
    onCancelHandlerRef.set(Some(onCancelHandler))

  val onCloseHandlerRef = new AtomicReference[Option[Runnable]](None)
  override def setOnCloseHandler(onCloseHandler: Runnable): Unit =
    onCloseHandlerRef.set(Some(onCloseHandler))

  override def isReady: Boolean = readinessSignalled.get()

  val onReadyHandlerRef = new AtomicReference[Option[Runnable]](None)
  override def setOnReadyHandler(onReadyHandler: Runnable): Unit = {
    onReadyHandlerRef.set(Some(onReadyHandler))
    if (counter.get > 0) signalReady()
  }

  private val manualFlowControl = new AtomicBoolean(false)
  override def disableAutoInboundFlowControl(): Unit = manualFlowControl.set(true)

  private val readinessSignalled = new AtomicBoolean(false)
  override def onNext(value: T): Unit = {
    checkInitialized()
    require(readinessSignalled.compareAndSet(true, false), "onNext called without ready signal")
    super.onNext(value)
    val newCounter = counter.decrementAndGet()
    if (newCounter == 0) {
      promise.trySuccess(values)
      cancel()
    } else if (newCounter > 0) {
      signalReady()
    }
  }

  override def onError(t: Throwable): Unit = {
    checkInitialized()
    super.onError(t)
    promise.tryFailure(t)
  }

  override def onCompleted(): Unit = {
    checkInitialized()
    super.onCompleted()
  }

  private def signalReady(): Unit = {
    onReadyHandlerRef.get.foreach(_.run())
    require(readinessSignalled.compareAndSet(false, true), "readiness has already been signalled")
  }

  private def cancel(): Unit = {
    isCancelled_ = true
    onCancelHandlerRef.get.foreach(_.run())
  }

  private def checkInitialized() = {
    require(
      onReadyHandlerRef.get().nonEmpty,
      "setOnReadyHandler() was not called by the service handler",
    )

    require(
      manualFlowControl.get,
      "disableAutoInboundFlowControl() was not called by the service handler",
    )
  }

  override def setCompression(compression: String): Unit = ???
  override def request(count: Int): Unit = ???
  override def setMessageCompression(enable: Boolean): Unit = ???
}
