// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package inferredspan

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/DataDog/datadog-agent/pkg/trace/pb"
	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
)

const (
	dataFile = "../testdata/event_samples/"
)

func TestSetSynchronicityFalse(t *testing.T) {
	var attributes EventKeys
	var span InferredSpan
	attributes.Headers.InvocationType = ""
	span.GenerateInferredSpan(time.Now())
	span.IsAsync = isAsyncEvent(attributes)

	assert.False(t, span.IsAsync)
}

func TestSetSynchronicityTrue(t *testing.T) {
	var attributes EventKeys
	var span InferredSpan
	attributes.Headers.InvocationType = "Event"
	span.GenerateInferredSpan(time.Now())
	span.IsAsync = isAsyncEvent(attributes)

	assert.True(t, span.IsAsync)
}

func TestEnrichInferredSpanWithAPIGatewayRESTEvent(t *testing.T) {
	var apiGatewayRestEvent events.APIGatewayProxyRequest
	_ = json.Unmarshal(getEventFromFile("api-gateway.json"), &apiGatewayRestEvent)

	inferredSpan := mockInferredSpan()
	inferredSpan.EnrichInferredSpanWithAPIGatewayRESTEvent(apiGatewayRestEvent)

	span := inferredSpan.Span

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1428582896000000000), span.Start)
	assert.Equal(t, "70ixmpl4fl.execute-api.us-east-2.amazonaws.com", span.Service)
	assert.Equal(t, "aws.apigateway", span.Name)
	assert.Equal(t, "POST /path/to/resource", span.Resource)
	assert.Equal(t, "http", span.Type)
	assert.Equal(t, "1234567890", span.Meta[APIID])
	assert.Equal(t, "1234567890", span.Meta[APIName])
	assert.Equal(t, "/path/to/resource", span.Meta[Endpoint])
	assert.Equal(t, "70ixmpl4fl.execute-api.us-east-2.amazonaws.com/path/to/resource", span.Meta[HTTPURL])
	assert.Equal(t, "aws.apigateway.rest", span.Meta[OperationName])
	assert.Equal(t, "c6af9ac6-7b61-11e6-9a41-93e8deadbeef", span.Meta[RequestID])
	assert.Equal(t, "POST /path/to/resource", span.Meta[ResourceNames])
	assert.Equal(t, "prod", span.Meta[Stage])
	assert.False(t, inferredSpan.IsAsync)
}

func TestEnrichInferredSpanWithAPIGatewayNonProxyAsyncRESTEvent(t *testing.T) {
	var apiGatewayRestEvent events.APIGatewayProxyRequest
	_ = json.Unmarshal(getEventFromFile("api-gateway-non-proxy-async.json"), &apiGatewayRestEvent)
	inferredSpan := mockInferredSpan()
	inferredSpan.EnrichInferredSpanWithAPIGatewayRESTEvent(apiGatewayRestEvent)

	span := inferredSpan.Span
	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1631210915251000000), span.Start)
	assert.Equal(t, "lgxbo6a518.execute-api.sa-east-1.amazonaws.com", span.Service)
	assert.Equal(t, "aws.apigateway", span.Name)
	assert.Equal(t, "GET /http/get", span.Resource)
	assert.Equal(t, "http", span.Type)
	assert.Equal(t, "lgxbo6a518", span.Meta[APIID])
	assert.Equal(t, "lgxbo6a518", span.Meta[APIName])
	assert.Equal(t, "/http/get", span.Meta[Endpoint])
	assert.Equal(t, "lgxbo6a518.execute-api.sa-east-1.amazonaws.com/http/get", span.Meta[HTTPURL])
	assert.Equal(t, "aws.apigateway.rest", span.Meta[OperationName])
	assert.Equal(t, "7bf3b161-f698-432c-a639-6fef8b445137", span.Meta[RequestID])
	assert.Equal(t, "GET /http/get", span.Meta[ResourceNames])
	assert.Equal(t, "dev", span.Meta[Stage])
	assert.True(t, inferredSpan.IsAsync)
}

func TestEnrichInferredSpanWithAPIGatewayHTTPEvent(t *testing.T) {
	var apiGatewayHTTPEvent events.APIGatewayV2HTTPRequest
	_ = json.Unmarshal(getEventFromFile("http-api.json"), &apiGatewayHTTPEvent)
	inferredSpan := mockInferredSpan()
	inferredSpan.EnrichInferredSpanWithAPIGatewayHTTPEvent(apiGatewayHTTPEvent)

	span := inferredSpan.Span
	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1631212283738000000), span.Start)
	assert.Equal(t, "x02yirxc7a.execute-api.sa-east-1.amazonaws.com", span.Service)
	assert.Equal(t, "aws.httpapi", span.Name)
	assert.Equal(t, "GET /httpapi/get", span.Resource)
	assert.Equal(t, "http", span.Type)
	assert.Equal(t, "GET", span.Meta[HTTPMethod])
	assert.Equal(t, "HTTP/1.1", span.Meta[HTTPProtocol])
	assert.Equal(t, "38.122.226.210", span.Meta[HTTPSourceIP])
	assert.Equal(t, "x02yirxc7a.execute-api.sa-east-1.amazonaws.com/httpapi/get", span.Meta[HTTPURL])
	assert.Equal(t, "curl/7.64.1", span.Meta[HTTPUserAgent])
	assert.Equal(t, "aws.httpapi", span.Meta[OperationName])
	assert.Equal(t, "FaHnXjKCGjQEJ7A=", span.Meta[RequestID])
	assert.Equal(t, "GET /httpapi/get", span.Meta[ResourceNames])
}

func TestEnrichInferredSpanWithAPIGatewayWebsocketDefaultEvent(t *testing.T) {
	var apiGatewayWebsocketEvent events.APIGatewayWebsocketProxyRequest
	_ = json.Unmarshal(getEventFromFile("api-gateway-websocket-default.json"), &apiGatewayWebsocketEvent)
	inferredSpan := mockInferredSpan()
	span := inferredSpan.Span

	inferredSpan.EnrichInferredSpanWithAPIGatewayWebsocketEvent(apiGatewayWebsocketEvent)

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1631285061365000000), span.Start)
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com", span.Service)
	assert.Equal(t, "aws.apigateway.websocket", span.Name)
	assert.Equal(t, "$default", span.Resource)
	assert.Equal(t, "web", span.Type)
	assert.Equal(t, "p62c47itsb", span.Meta[APIID])
	assert.Equal(t, "p62c47itsb", span.Meta[APIName])
	assert.Equal(t, "Fc5SzcoYGjQCJlg=", span.Meta[ConnectionID])
	assert.Equal(t, "$default", span.Meta[Endpoint])
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com$default", span.Meta[HTTPURL])
	assert.Equal(t, "IN", span.Meta[MessageDirection])
	assert.Equal(t, "aws.apigateway.websocket", span.Meta[OperationName])
	assert.Equal(t, "Fc5S3EvdGjQFtsQ=", span.Meta[RequestID])
	assert.Equal(t, "$default", span.Meta[ResourceNames])
	assert.Equal(t, "dev", span.Meta[Stage])
}

func TestEnrichInferredSpanWithAPIGatewayWebsocketConnectEvent(t *testing.T) {
	var apiGatewayWebsocketEvent events.APIGatewayWebsocketProxyRequest
	_ = json.Unmarshal(getEventFromFile("api-gateway-websocket-connect.json"), &apiGatewayWebsocketEvent)
	inferredSpan := mockInferredSpan()
	span := inferredSpan.Span

	inferredSpan.EnrichInferredSpanWithAPIGatewayWebsocketEvent(apiGatewayWebsocketEvent)

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1631284003071000000), span.Start)
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com", span.Service)
	assert.Equal(t, "aws.apigateway.websocket", span.Name)
	assert.Equal(t, "$connect", span.Resource)
	assert.Equal(t, "web", span.Type)
	assert.Equal(t, "p62c47itsb", span.Meta[APIID])
	assert.Equal(t, "p62c47itsb", span.Meta[APIName])
	assert.Equal(t, "Fc2tgfl3mjQCJfA=", span.Meta[ConnectionID])
	assert.Equal(t, "$connect", span.Meta[Endpoint])
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com$connect", span.Meta[HTTPURL])
	assert.Equal(t, "IN", span.Meta[MessageDirection])
	assert.Equal(t, "aws.apigateway.websocket", span.Meta[OperationName])
	assert.Equal(t, "Fc2tgH1RmjQFnOg=", span.Meta[RequestID])
	assert.Equal(t, "$connect", span.Meta[ResourceNames])
	assert.Equal(t, "dev", span.Meta[Stage])
}

func TestEnrichInferredSpanWithAPIGatewayWebsocketDisconnectEvent(t *testing.T) {
	var apiGatewayWebsocketEvent events.APIGatewayWebsocketProxyRequest
	_ = json.Unmarshal(getEventFromFile("api-gateway-websocket-disconnect.json"), &apiGatewayWebsocketEvent)
	inferredSpan := mockInferredSpan()
	span := inferredSpan.Span

	inferredSpan.EnrichInferredSpanWithAPIGatewayWebsocketEvent(apiGatewayWebsocketEvent)

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, int64(1631284034737000000), span.Start)
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com", span.Service)
	assert.Equal(t, "aws.apigateway.websocket", span.Name)
	assert.Equal(t, "$disconnect", span.Resource)
	assert.Equal(t, "web", span.Type)
	assert.Equal(t, "p62c47itsb", span.Meta[APIID])
	assert.Equal(t, "p62c47itsb", span.Meta[APIName])
	assert.Equal(t, "Fc2tgfl3mjQCJfA=", span.Meta[ConnectionID])
	assert.Equal(t, "$disconnect", span.Meta[Endpoint])
	assert.Equal(t, "p62c47itsb.execute-api.sa-east-1.amazonaws.com$disconnect", span.Meta[HTTPURL])
	assert.Equal(t, "IN", span.Meta[MessageDirection])
	assert.Equal(t, "aws.apigateway.websocket", span.Meta[OperationName])
	assert.Equal(t, "Fc2ydE4LmjQFhdg=", span.Meta[RequestID])
	assert.Equal(t, "$disconnect", span.Meta[ResourceNames])
	assert.Equal(t, "dev", span.Meta[Stage])
}

func TestEnrichInferredSpanWithSNSEvent(t *testing.T) {
	var snsRequest events.SNSEvent
	_ = json.Unmarshal(getEventFromFile("sns.json"), &snsRequest)
	inferredSpan := mockInferredSpan()
	inferredSpan.EnrichInferredSpanWithSNSEvent(snsRequest)

	span := inferredSpan.Span

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, formatISOStartTime("2022-01-31T14:13:41.637Z"), span.Start)
	assert.Equal(t, "sns", span.Service)
	assert.Equal(t, "aws.sns", span.Name)
	assert.Equal(t, "serverlessTracingTopicPy", span.Resource)
	assert.Equal(t, "web", span.Type)
	assert.Equal(t, "87056a47-f506-5d77-908b-303605d3b197", span.Meta[MessageID])
	assert.Equal(t, "aws.sns", span.Meta[OperationName])
	assert.Equal(t, "serverlessTracingTopicPy", span.Meta[ResourceNames])
	assert.Equal(t, "Hello", span.Meta[Subject])
	assert.Equal(t, "arn:aws:sns:sa-east-1:601427279990:serverlessTracingTopicPy", span.Meta[TopicARN])
	assert.Equal(t, "serverlessTracingTopicPy", span.Meta[TopicName])
	assert.Equal(t, "Notification", span.Meta[Type])
	assert.True(t, inferredSpan.IsAsync)
}

func TestEnrichInferredSpanWithKinesisEvent(t *testing.T) {
	var kinesisRequest events.KinesisEvent
	_ = json.Unmarshal(getEventFromFile("kinesis.json"), &kinesisRequest)
	inferredSpan := mockInferredSpan()
	inferredSpan.EnrichInferredSpanWithKinesisEvent(kinesisRequest)

	span := inferredSpan.Span

	assert.Equal(t, uint64(7353030974370088224), span.TraceID)
	assert.Equal(t, uint64(8048964810003407541), span.SpanID)
	assert.Equal(t, "kinesis", span.Service)
	assert.Equal(t, "aws.kinesis", span.Name)
	assert.Equal(t, "stream/kinesisStream", span.Resource)
	assert.Equal(t, WEB, span.Type)
	assert.Equal(t, "aws.kinesis", span.Meta[OperationName])
	assert.Equal(t, "stream/kinesisStream", span.Meta[ResourceNames])
	assert.Equal(t, "stream/kinesisStream", span.Meta[StreamName])
	assert.Equal(t, "shardId-000000000002", span.Meta[ShardID])
	assert.Equal(t, "arn:aws:kinesis:sa-east-1:601427279990:stream/kinesisStream", span.Meta[EventSourceARN])
	assert.Equal(t, "shardId-000000000002:49624230154685806402418173680709770494154422022871973922", span.Meta[EventID])
	assert.Equal(t, "aws:kinesis:record", span.Meta[EventName])
	assert.Equal(t, "1.0", span.Meta[EventVersion])
	assert.Equal(t, "partitionkey", span.Meta[PartitionKey])
	assert.True(t, inferredSpan.IsAsync)
}

// ```def test_extract_context_from_kinesis_batch_event(self):
//         event_sample_source = "kinesis-batch"
//         test_file = event_samples + event_sample_source + ".json"
//         with open(test_file, "r") as event:
//             event = json.load(event)
//         ctx = get_mock_context()
//         context, source = extract_dd_trace_context(event, ctx)
//         self.assertEqual(context["trace-id"], "4948377316357291421")
//         self.assertEqual(context["parent-id"], "2876253380018681026")
//         self.assertEqual(context["sampling-priority"], "1")```

func TestFormatISOStartTime(t *testing.T) {
	isotime := "2022-01-31T14:13:41.637Z"
	startTime := formatISOStartTime(isotime)
	assert.Equal(t, int64(1643638421637000000), startTime)

}

func TestFormatInvalidISOStartTime(t *testing.T) {
	isotime := "invalid"
	startTime := formatISOStartTime(isotime)
	assert.Equal(t, int64(0), startTime)
}

func getEventFromFile(filename string) []byte {
	event, _ := os.ReadFile(dataFile + filename)
	return event
}

func mockInferredSpan() InferredSpan {
	var inferredSpan InferredSpan
	inferredSpan.Span = &pb.Span{}
	inferredSpan.Span.TraceID = uint64(7353030974370088224)
	inferredSpan.Span.SpanID = uint64(8048964810003407541)
	return inferredSpan
}

func TestCalculateStartTime(t *testing.T) {
	assert.Equal(t, int64(1651863561696000000), calculateStartTime(1651863561696))
}
