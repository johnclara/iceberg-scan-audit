package com.box.dataplatform.iceberg.client.failures.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.concurrent.duration._

/**
 *
 */
class BreakableDynamoDB(realDynamoDB: AmazonDynamoDB) {
  private var timeout = false
  private var throwing = false

  private val answer = new Answer[Object] {
    def answer(invocation: InvocationOnMock): Object =
      if (timeout) {
        Thread.sleep(5.minutes.toMillis)
        return null;
      } else if (throwing) {
        throw new AmazonDynamoDBException("throwing")
      } else {
        Mockito.CALLS_REAL_METHODS.answer(invocation)
      }
  }

  private val settings = Mockito.withSettings
    .spiedInstance(realDynamoDB)
    .defaultAnswer(answer)

  def fix(): Unit = {
    timeout = false
    throwing = false
  }

  def startThrowing(): Unit =
    throwing = true

  def startTimingOut(): Unit =
    timeout = true

  // must use the runtime class since the realDynamoDB is actually a com.sun.proxy.$Proxy
  val mock: AmazonDynamoDB = Mockito.mock(realDynamoDB.getClass, settings)
}
