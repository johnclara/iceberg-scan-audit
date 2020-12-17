package com.box.dataplatform.iceberg.client.failures.s3

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.duration._

/**
 *
 */
object BreakableS3Factory {
  private var timeout = false
  private var throwing = false

  private val answer = new Answer[Object] {
    def answer(invocation: InvocationOnMock): Object =
      if (timeout) {
        Thread.sleep(5.minutes.toMillis)
        return null;
      } else if (throwing) {
        throw new SdkClientException("throwing")
      } else {
        Mockito.CALLS_REAL_METHODS.answer(invocation)
      }
  }

  def fix(): Unit = {
    timeout = false
    throwing = false
  }

  def startThrowing(): Unit =
    throwing = true

  def startTimingOut(): Unit =
    timeout = true

  def wrap(realS3: AmazonS3): AmazonS3 = {
    val settings = Mockito.withSettings
      .spiedInstance(realS3)
      .defaultAnswer(answer)

    Mockito.mock(realS3.getClass, settings)
  }
}
