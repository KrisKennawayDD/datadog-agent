// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package inferredspan

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/datadog-agent/pkg/util/log"
	"github.com/aws/aws-lambda-go/events"
)

// EnrichInferredSpanWithAPIGatewayRESTEvent uses the parsed event
// payload to enrich the current inferred span. It applies a
// specific set of data to the span expected from a REST event.
func (inferredSpan *InferredSpan) EnrichInferredSpanWithAPIGatewayRESTEvent(eventPayload events.APIGatewayProxyRequest) {
	log.Debug("Enriching an inferred span for a REST API Gateway")
	requestContext := eventPayload.RequestContext
	resource := fmt.Sprintf("%s %s", eventPayload.HTTPMethod, eventPayload.Path)
	httpurl := fmt.Sprintf("%s%s", requestContext.DomainName, eventPayload.Path)
	startTime := calculateStartTime(requestContext.RequestTimeEpoch)

	inferredSpan.Span.Name = "aws.apigateway"
	inferredSpan.Span.Service = requestContext.DomainName
	inferredSpan.Span.Resource = resource
	inferredSpan.Span.Start = startTime
	inferredSpan.Span.Type = "http"
	inferredSpan.Span.Meta = map[string]string{
		APIID:         requestContext.APIID,
		APIName:       requestContext.APIID,
		Endpoint:      eventPayload.Path,
		HTTPURL:       httpurl,
		OperationName: "aws.apigateway.rest",
		RequestID:     requestContext.RequestID,
		ResourceNames: resource,
		Stage:         requestContext.Stage,
	}

	inferredSpan.IsAsync = eventPayload.Headers[InvocationType] == "Event"
}

// EnrichInferredSpanWithAPIGatewayHTTPEvent uses the parsed event
// payload to enrich the current inferred span. It applies a
// specific set of data to the span expected from a HTTP event.
func (inferredSpan *InferredSpan) EnrichInferredSpanWithAPIGatewayHTTPEvent(eventPayload events.APIGatewayV2HTTPRequest) {
	log.Debug("Enriching an inferred span for a HTTP API Gateway")
	requestContext := eventPayload.RequestContext
	http := requestContext.HTTP
	path := eventPayload.RequestContext.HTTP.Path
	resource := fmt.Sprintf("%s %s", http.Method, path)
	httpurl := fmt.Sprintf("%s%s", requestContext.DomainName, path)
	startTime := calculateStartTime(requestContext.TimeEpoch)

	inferredSpan.Span.Name = "aws.httpapi"
	inferredSpan.Span.Service = requestContext.DomainName
	inferredSpan.Span.Resource = resource
	inferredSpan.Span.Type = "http"
	inferredSpan.Span.Start = startTime
	inferredSpan.Span.Meta = map[string]string{
		Endpoint:      path,
		HTTPURL:       httpurl,
		HTTPMethod:    http.Method,
		HTTPProtocol:  http.Protocol,
		HTTPSourceIP:  http.SourceIP,
		HTTPUserAgent: http.UserAgent,
		OperationName: "aws.httpapi",
		RequestID:     requestContext.RequestID,
		ResourceNames: resource,
	}

	inferredSpan.IsAsync = eventPayload.Headers[InvocationType] == "Event"
}

// EnrichInferredSpanWithAPIGatewayWebsocketEvent uses the parsed event
// payload to enrich the current inferred span. It applies a
// specific set of data to the span expected from a Websocket event.
func (inferredSpan *InferredSpan) EnrichInferredSpanWithAPIGatewayWebsocketEvent(eventPayload events.APIGatewayWebsocketProxyRequest) {
	log.Debug("Enriching an inferred span for a Websocket API Gateway")
	requestContext := eventPayload.RequestContext
	endpoint := requestContext.RouteKey
	httpurl := fmt.Sprintf("%s%s", requestContext.DomainName, endpoint)
	startTime := calculateStartTime(requestContext.RequestTimeEpoch)

	inferredSpan.Span.Name = "aws.apigateway.websocket"
	inferredSpan.Span.Service = requestContext.DomainName
	inferredSpan.Span.Resource = endpoint
	inferredSpan.Span.Type = "web"
	inferredSpan.Span.Start = startTime
	inferredSpan.Span.Meta = map[string]string{
		APIID:            requestContext.APIID,
		APIName:          requestContext.APIID,
		ConnectionID:     requestContext.ConnectionID,
		Endpoint:         endpoint,
		EventType:        requestContext.EventType,
		HTTPURL:          httpurl,
		MessageDirection: requestContext.MessageDirection,
		OperationName:    "aws.apigateway.websocket",
		RequestID:        requestContext.RequestID,
		ResourceNames:    endpoint,
		Stage:            requestContext.Stage,
	}

	inferredSpan.IsAsync = eventPayload.Headers[InvocationType] == "Event"
}

func (inferredSpan *InferredSpan) EnrichInferredSpanWithSNSEvent(eventPayload events.SNSEvent) {
	eventRecord := eventPayload.Records[0]
	snsMessage := eventRecord.SNS
	splitArn := strings.Split(snsMessage.TopicArn, ":")
	topicName := splitArn[len(splitArn)-1]
	startTime := snsMessage.Timestamp.UnixNano()

	inferredSpan.IsAsync = true
	inferredSpan.Span.Name = "aws.sns"
	inferredSpan.Span.Service = SNS
	inferredSpan.Span.Start = startTime
	inferredSpan.Span.Resource = topicName
	inferredSpan.Span.Type = "web"
	inferredSpan.Span.Meta = map[string]string{
		OperationName: "aws.sns",
		ResourceNames: topicName,
		TopicName:     topicName,
		TopicARN:      snsMessage.TopicArn,
		MessageID:     snsMessage.MessageID,
		Type:          snsMessage.Type,
	}

	//Subject not available in SNS => SQS scenario
	if snsMessage.Subject != "" {
		inferredSpan.Span.Meta[Subject] = snsMessage.Subject
	}
}

// EnrichInferredSpanWithDynamoDBEvent uses the parsed event
// payload to enrich the current inferred span. It applies a
// specific set of data to the span expected from a DynamoDB event.
func (inferredSpan *InferredSpan) EnrichInferredSpanWithDynamoDBEvent(eventPayload events.DynamoDBEvent) {
	eventRecord := eventPayload.Records[0]
	eventSourceArn := eventRecord.EventSourceArn
	tableName := strings.Split(eventSourceArn, "/")[1]
	eventMessage := eventRecord.Change

	inferredSpan.IsAsync = true
	inferredSpan.Span.Name = "aws.dynamodb"
	inferredSpan.Span.Service = DYNAMODB
	inferredSpan.Span.Start = eventMessage.ApproximateCreationDateTime.UnixNano()
	inferredSpan.Span.Resource = tableName
	inferredSpan.Span.Type = WEB
	inferredSpan.Span.Meta = map[string]string{
		OperationName:  "aws.dynamodb",
		ResourceNames:  tableName,
		TableName:      tableName,
		EventSourceARN: eventSourceArn,
		EventID:        eventRecord.EventID,
		EventName:      eventRecord.EventName,
		EventVersion:   eventRecord.EventVersion,
		StreamViewType: eventRecord.Change.StreamViewType,
		SizeBytes:      strconv.FormatInt(eventRecord.Change.SizeBytes, 10),
	}
}

func isAsyncEvent(snsRequest EventKeys) bool {
	return snsRequest.Headers.InvocationType == "Event"
}

// CalculateStartTime converts AWS event timeEpochs to nanoseconds
func calculateStartTime(epoch int64) int64 {
	return epoch * 1e6
}

// formatISOStartTime converts ISO timestamps and returns
// a Unix timestamp in nanoseconds
func formatISOStartTime(isotime string) int64 {
	layout := "2006-01-02T15:04:05.000Z"
	startTime, err := time.Parse(layout, isotime)
	if err != nil {
		log.Debugf("Error parsing ISO time %s, failing with: %s", isotime, err)
		return 0
	}
	return startTime.UnixNano()
}
