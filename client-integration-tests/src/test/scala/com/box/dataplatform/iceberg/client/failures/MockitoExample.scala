package com.box.dataplatform.iceberg.client.failures

import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.specs2.SpecificationWithJUnit
import org.specs2.matcher.MustThrownMatchers
import org.specs2.mock.{Mockito => SpecsMockito}

/**
 * This test is an example on how to add a spy that flips to other defaults
 */
class MockitoExample extends SpecificationWithJUnit with MustThrownMatchers with SpecsMockito {
  def is =
    s2"""
            Settings must be immutable ${env().work}
      """

  class Leaders {
    def nicest: String = "John"
    def coolest: String = "JC"
  }

  case class env() {
    val realLeaders: Leaders = new Leaders();

    var broken = false
    val answer = new Answer[Object] {
      def answer(invocation: InvocationOnMock): Object =
        if (broken) {
          return "BEN"
        } else {
          Mockito.CALLS_REAL_METHODS.answer(invocation)
        }
    }

    val settings = Mockito.withSettings
      .spiedInstance(realLeaders)
      .defaultAnswer(answer)

    val fakeLeaders: Leaders = mock[Leaders](settings)

    def work = {
      fakeLeaders.nicest mustEqual "John"
      fakeLeaders.coolest mustEqual "JC"

      broken = true

      fakeLeaders.nicest mustEqual "BEN"
      fakeLeaders.coolest mustEqual "BEN"
    }
  }
}
