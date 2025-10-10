package uk.gov.justice.hmpps.sqs.telemetry

import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.services.sns.model.PublishBatchRequest
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry
import software.amazon.awssdk.services.sns.model.PublishRequest
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import uk.gov.justice.hmpps.sqs.eventTypeSnsMap
import uk.gov.justice.hmpps.sqs.eventTypeSqsMap

class TraceInjectingExecutionInterceptorTest {
  companion object {
    @RegisterExtension
    @JvmStatic
    val openTelemetryExtension: OpenTelemetryExtension = OpenTelemetryExtension.create()
  }

  private val interceptor = TraceInjectingExecutionInterceptor()

  @Test
  fun `should create span with correct attributes for SQS SendMessage`() {
    val request = SendMessageRequest.builder()
      .queueUrl("https://sqs.eu-west-2.amazonaws.com/123456789/test-queue")
      .messageBody("test message")
      .messageAttributes(eventTypeSqsMap("OFFENDER_MOVEMENT"))
      .build()

    val context = Context.modifyRequest().request(request).build()
    interceptor.modifyRequest(context, null)

    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)

    val span = spans[0]
    assertThat(span.name).isEqualTo("PUBLISH OFFENDER_MOVEMENT")
    assertThat(span.kind).isEqualTo(SpanKind.PRODUCER)
    
    // Verify OpenTelemetry messaging attributes
    val attributes = span.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws_sqs")
    assertThat(attributes["messaging.operation.type"]).isEqualTo("send")
    assertThat(attributes["messaging.operation.name"]).isEqualTo("sendMessage")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("test-queue")
    assertThat(attributes["server.address"]).isEqualTo("sqs.amazonaws.com")
    assertThat(attributes["server.port"]).isEqualTo(443)
    assertThat(attributes["messaging.message.conversation_id"]).isEqualTo("OFFENDER_MOVEMENT")
  }

  @Test
  fun `should create span with correct attributes for SQS SendMessageBatch`() {
    val request = SendMessageBatchRequest.builder()
      .queueUrl("https://sqs.eu-west-2.amazonaws.com/123456789/batch-queue")
      .entries(
        SendMessageBatchRequestEntry.builder()
          .id("1")
          .messageBody("message 1")
          .messageAttributes(eventTypeSqsMap("EVENT_1"))
          .build(),
        SendMessageBatchRequestEntry.builder()
          .id("2") 
          .messageBody("message 2")
          .messageAttributes(eventTypeSqsMap("EVENT_2"))
          .build()
      )
      .build()

    val context = Context.modifyRequest().request(request).build()
    interceptor.modifyRequest(context, null)

    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)

    val span = spans[0]
    assertThat(span.name).isEqualTo("PUBLISH")
    assertThat(span.kind).isEqualTo(SpanKind.PRODUCER)
    
    val attributes = span.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws_sqs")
    assertThat(attributes["messaging.operation.type"]).isEqualTo("send")
    assertThat(attributes["messaging.operation.name"]).isEqualTo("sendMessageBatch")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("batch-queue")
    assertThat(attributes["messaging.batch.message_count"]).isEqualTo(2)
    assertThat(attributes["server.address"]).isEqualTo("sqs.amazonaws.com")
    assertThat(attributes["server.port"]).isEqualTo(443)
  }

  @Test
  fun `should create span with correct attributes for SNS Publish`() {
    val request = PublishRequest.builder()
      .topicArn("arn:aws:sns:eu-west-2:123456789:prison-events")
      .message("test message")
      .messageAttributes(eventTypeSnsMap("PRISONER_RELEASED"))
      .build()

    val context = Context.modifyRequest().request(request).build()
    interceptor.modifyRequest(context, null)

    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)

    val span = spans[0]
    assertThat(span.name).isEqualTo("PUBLISH PRISONER_RELEASED")
    assertThat(span.kind).isEqualTo(SpanKind.PRODUCER)
    
    val attributes = span.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws.sns")
    assertThat(attributes["messaging.operation.type"]).isEqualTo("send")
    assertThat(attributes["messaging.operation.name"]).isEqualTo("publish")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("prison-events")
    assertThat(attributes["server.address"]).isEqualTo("sns.amazonaws.com")
    assertThat(attributes["server.port"]).isEqualTo(443)
    assertThat(attributes["messaging.message.conversation_id"]).isEqualTo("PRISONER_RELEASED")
  }

  @Test
  fun `should create span with correct attributes for SNS PublishBatch`() {
    val request = PublishBatchRequest.builder()
      .topicArn("arn:aws:sns:eu-west-2:123456789:batch-events")
      .publishBatchRequestEntries(
        PublishBatchRequestEntry.builder()
          .id("1")
          .message("message 1")
          .messageAttributes(eventTypeSnsMap("EVENT_A"))
          .build(),
        PublishBatchRequestEntry.builder()
          .id("2")
          .message("message 2") 
          .messageAttributes(eventTypeSnsMap("EVENT_B"))
          .build(),
        PublishBatchRequestEntry.builder()
          .id("3")
          .message("message 3")
          .messageAttributes(eventTypeSnsMap("EVENT_C"))
          .build()
      )
      .build()

    val context = Context.modifyRequest().request(request).build()
    interceptor.modifyRequest(context, null)

    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)

    val span = spans[0]
    assertThat(span.name).isEqualTo("PUBLISH")
    assertThat(span.kind).isEqualTo(SpanKind.PRODUCER)
    
    val attributes = span.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws.sns")
    assertThat(attributes["messaging.operation.type"]).isEqualTo("send")
    assertThat(attributes["messaging.operation.name"]).isEqualTo("publishBatch")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("batch-events")
    assertThat(attributes["messaging.batch.message_count"]).isEqualTo(3)
    assertThat(attributes["server.address"]).isEqualTo("sns.amazonaws.com")
    assertThat(attributes["server.port"]).isEqualTo(443)
  }

  @Test
  fun `should handle requests without eventType gracefully`() {
    val request = SendMessageRequest.builder()
      .queueUrl("https://sqs.eu-west-2.amazonaws.com/123456789/no-event-queue")
      .messageBody("test message without event type")
      .build()

    val context = Context.modifyRequest().request(request).build()
    interceptor.modifyRequest(context, null)

    val spans = openTelemetryExtension.spans
    assertThat(spans).hasSize(1)

    val span = spans[0]
    assertThat(span.name).isEqualTo("PUBLISH")
    assertThat(span.kind).isEqualTo(SpanKind.PRODUCER)
    
    val attributes = span.attributes.asMap()
    assertThat(attributes["messaging.system"]).isEqualTo("aws_sqs")
    assertThat(attributes["messaging.destination.name"]).isEqualTo("no-event-queue")
    assertThat(attributes).doesNotContainKey("messaging.message.conversation_id")
  }
}