package uk.gov.justice.hmpps.sqs.telemetry

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.ObjectMapper
import io.awspring.cloud.sqs.listener.SqsHeaders
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.json.JsonTest
import org.springframework.messaging.support.GenericMessage
import software.amazon.awssdk.services.sqs.model.Message
import uk.gov.justice.hmpps.sqs.eventTypeSqsMap
import uk.gov.justice.hmpps.sqs.findLogAppender
import uk.gov.justice.hmpps.sqs.formattedMessages

@JsonTest
class TraceExtractingMessageInterceptorTest(@Autowired private val objectMapper: ObjectMapper) {
  companion object {
    @RegisterExtension
    @JvmStatic
    val openTelemetryExtension: OpenTelemetryExtension = OpenTelemetryExtension.create()
  }

  private lateinit var logger: ListAppender<ILoggingEvent>

  @BeforeEach
  fun setupLogAppender() {
    logger = findLogAppender(TraceExtractingMessageInterceptor::class.java)
  }

  @Test
  fun `should do nothing if no MessageAttributes header found`() {
    val message = GenericMessage<Any>("123", mapOf("eventType" to "myevent"))

    val responseMessage = TraceExtractingMessageInterceptor(objectMapper).intercept(message)

    assertThat(responseMessage.headers).isEqualTo(message.headers)
    // no new span will be created, so don't expect these keys
    assertThat(responseMessage.headers).doesNotContainKeys("span", "scope")

    // and log message informing us that it couldn't find the header message attributes
    assertThat(logger.formattedMessages()).anyMatch { it.contains("Unable to find header") }
  }

  @Test
  fun `should start a new span if MessageAttributes payload contains MessageAttributes`() {
    val message = GenericMessage<Any>("""{"MessageId":null,"MessageAttributes":{"my-event":{"Type": "String", "Value": "my-event"}}}""", mapOf("eventType" to "myevent"))

    val responseMessage = TraceExtractingMessageInterceptor(objectMapper).intercept(message)

    assertThat(responseMessage.headers).containsAllEntriesOf(message.headers)

    // this shows that we have created a new span
    assertThat(responseMessage.headers).containsKeys("span", "scope")

    // and no failure log messages
    assertThat(logger.formattedMessages()).isEmpty()
  }

  @Test
  fun `should do nothing if MessageAttributes payload is null`() {
    val message = GenericMessage<Any>("""{"MessageId":null,"MessageAttributes":null}""", mapOf("eventType" to "myevent"))

    val responseMessage = TraceExtractingMessageInterceptor(objectMapper).intercept(message)

    assertThat(responseMessage.headers).isEqualTo(message.headers)

    // no new span will be created, so don't expect these keys
    assertThat(responseMessage.headers).doesNotContainKeys("span", "scope")

    // and log message informing us that it couldn't find the header message attributes
    assertThat(logger.formattedMessages()).anyMatch { it.contains("Unable to find header") }
  }

  @Test
  fun `should do nothing if MessageAttributes payload is invalid json`() {
    val message = GenericMessage<Any>("""{invalid json "MessageAttributes":null""", mapOf("eventType" to "myevent"))

    val responseMessage = TraceExtractingMessageInterceptor(objectMapper).intercept(message)

    assertThat(responseMessage.headers).isEqualTo(message.headers)

    // no new span will be created, so don't expect these keys
    assertThat(responseMessage.headers).doesNotContainKeys("span", "scope")

    // but we do get a failure log message
    assertThat(logger.formattedMessages()).anyMatch { it.contains("Not attempting to extract trace context from message") }
  }

  @Test
  fun `should start a new spa if MessageAttributes payload is null but message attributes exist in header`() {
    val message = GenericMessage<Any>(
      """{"MessageId":null,"MessageAttributes":null}""",
      mapOf("eventType" to "myevent", SqsHeaders.SQS_SOURCE_DATA_HEADER to Message.builder().messageAttributes(eventTypeSqsMap("myevent")).build()),
    )

    val responseMessage = TraceExtractingMessageInterceptor(objectMapper).intercept(message)

    assertThat(responseMessage.headers).containsAllEntriesOf(message.headers)

    // this shows that we have created a new span
    assertThat(responseMessage.headers).containsKeys("span", "scope")

    // and no failure log messages
    assertThat(logger.formattedMessages()).isEmpty()
  }

  @Test
  fun `should set correct OpenTelemetry attributes when processing message with afterProcessing`() {
    val sqsMessage = Message.builder()
      .messageId("test-message-id-123")
      .messageAttributes(eventTypeSqsMap("PRISONER_RECEIVED"))
      .build()

    val message = GenericMessage<Any>(
      "test message body",
      mapOf(
        "eventType" to "PRISONER_RECEIVED",
        SqsHeaders.SQS_SOURCE_DATA_HEADER to sqsMessage,
        SqsHeaders.SQS_QUEUE_URL_HEADER to "https://sqs.eu-west-2.amazonaws.com/123456789/prisoner-events-queue"
      )
    )

    val interceptor = TraceExtractingMessageInterceptor(objectMapper)
    
    // First intercept the message to create span
    val interceptedMessage = interceptor.intercept(message)
    
    // Verify span was created
    assertThat(interceptedMessage.headers).containsKeys("span", "scope")
    val span = interceptedMessage.headers["span"] as Span
    
    // Simulate successful processing (no exception)
    interceptor.afterProcessing(interceptedMessage, null)
    
    // Check that span was ended and attributes were set
    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)
    
    val recordedSpan = spans[0]
    assertThat(recordedSpan.kind).isEqualTo(SpanKind.CONSUMER)
    
    val attributes = recordedSpan.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws_sqs")
    assertThat(attributes["messaging.operation.type"]).isEqualTo("receive")
    assertThat(attributes["messaging.operation.name"]).isEqualTo("receiveMessage")
    assertThat(attributes["messaging.message.id"]).isEqualTo("test-message-id-123")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("prisoner-events-queue")
    assertThat(attributes["server.address"]).isEqualTo("sqs.amazonaws.com")
    assertThat(attributes["server.port"]).isEqualTo(443)
    assertThat(attributes["messaging.message.conversation_id"]).isEqualTo("PRISONER_RECEIVED")
  }

  @Test
  fun `should handle missing queue URL gracefully`() {
    val sqsMessage = Message.builder()
      .messageId("test-message-id-456")
      .messageAttributes(eventTypeSqsMap("TEST_EVENT"))
      .build()

    val message = GenericMessage<Any>(
      "test message body",
      mapOf(
        SqsHeaders.SQS_SOURCE_DATA_HEADER to sqsMessage
        // Note: no SqsHeaders.SQS_QUEUE_URL_HEADER
      )
    )

    val interceptor = TraceExtractingMessageInterceptor(objectMapper)
    val interceptedMessage = interceptor.intercept(message)
    interceptor.afterProcessing(interceptedMessage, null)
    
    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)
    
    val attributes = spans[0].attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws_sqs")
    assertThat(attributes["messaging.message.id"]).isEqualTo("test-message-id-456")
    // Should not have destination name when queue URL is missing
    assertThat(attributes).doesNotContainKey("messaging.destination.name")
    assertThat(attributes["messaging.message.conversation_id"]).isEqualTo("TEST_EVENT")
  }
}
