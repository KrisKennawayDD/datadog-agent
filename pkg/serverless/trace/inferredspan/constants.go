// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

package inferredspan

const (
	// APIID and below are used for inferred span
	// tagging and enrichment
	APIID            = "apiid"
	APIName          = "apiname"
	BucketARN        = "bucket_arn"
	BucketName       = "bucketname"
	ConnectionID     = "connection_id"
	Endpoint         = "endpoint"
	EventName        = "event_name"
	EventType        = "event_type"
	HTTP             = "http"
	HTTPURL          = "http.url"
	HTTPMethod       = "http.method"
	HTTPProtocol     = "http.protocol"
	HTTPSourceIP     = "http.source_ip"
	HTTPUserAgent    = "http.user_agent"
	MessageDirection = "message_direction"
	MessageID        = "message_id"
	ObjectKey        = "object_key"
	ObjectSize       = "object_size"
	ObjectETag       = "object_etag"
	OperationName    = "operation_name"
	RequestID        = "request_id"
	ResourceNames    = "resource_names"
	Stage            = "stage"
	Subject          = "subject"
	TopicName        = "topicname"
	TopicARN         = "topic_arn"
	Type             = "type"

	InvocationType = "X-Amz-Invocation-Type"
	// APIGATEWAY and below are used for parsing
	// and setting the event sources
	APIGATEWAY = "apigateway"
	HTTPAPI    = "http-api"
	S3         = "s3"
	SNS        = "sns"
	SNSType    = "aws:sns"
	WEBSOCKET  = "websocket"
	UNKNOWN    = "unknown"
)

// Not in library
type EventBridgeEvent struct {
}

// Not in Library
type LambdaFunctionURL struct {
	//not sure about this one??
	//    if request_context and request_context.get("stage"):
	//     if "domainName" in request_context and detect_lambda_function_url_domain(
	//         request_context.get("domainName")
	//     ):
	//         return _EventSource(EventTypes.LAMBDA_FUNCTION_URL)
}

////////////////////////////////////////////
///////////////// OLD CODE /////////////////
////////////////////////////////////////////
type APIGatewayBaseEvent struct {
	RequestContext RequestContextKeys `mapstructure:"requestContext" json:"requestContext"`
	Headers        HeaderKeys         `mapstructure:"headers" json:"headers"`
}

// APIGatewayRESTEvent is the API gateway request event
type APIGatewayRESTEvent struct {
	APIGatewayBaseEvent
	Path       string `mapstructure:"path" json:"path"`
	HTTPMethod string `mapstructure:"httpMethod" json:"httpMethod"`
}

type APIGatewayHTTPEvent struct {
	APIGatewayBaseEvent
}

type APIGatewayWebsocketEvent struct {
	APIGatewayBaseEvent
}

// SNSRequest is the SNS event
type SNSRequest struct {
	Records []*RecordKeys `mapStructure:"Records" json:"Records"`
}

// EventKeys are used to tell us what event type we received
type EventKeys struct {
	RequestContext RequestContextKeys `json:"requestContext"`
	Headers        HeaderKeys         `json:"headers"`
	Records        []*RecordKeys      `json:"Records"`
	HTTPMethod     string             `json:"httpMethod"`
	Path           string             `json:"path"`
}

// RequestContextKeys holds the nested requestContext from the payload.
type RequestContextKeys struct {
	Stage            string   `json:"stage"`
	RouteKey         string   `json:"routeKey"`
	MessageDirection string   `json:"messageDirection"`
	Domain           string   `json:"domainName"`
	APIID            string   `json:"apiId"`
	RawPath          string   `json:"rawPath"`
	RequestID        string   `json:"requestID"`
	RequestTimeEpoch int64    `json:"requestTimeEpoch"`
	HTTP             HTTPKeys `json:"http"`
	ConnectionID     string   `json:"connectionId"`
	EventType        string   `json:"eventType"`
	TimeEpoch        int64    `json:"timeEpoch"`
}

// HeaderKeys holds the extracted headers from the trace context
type HeaderKeys struct {
	InvocationType string `json:"X-Amz-Invocation-Type",mapstructure:"X-Amz-Invocation-Type"`
}

// HTTPKeys holds the nested HTTP data from the event payload
type HTTPKeys struct {
	Method    string `json:"method"`
	Protocol  string `json:"protocol"`
	SourceIP  string `json:"sourceIp"`
	UserAgent string `json:"userAgent"`
}

// RecordKeys holds the data for Records
type RecordKeys struct {
	EventSource string  `json:"EventSource"`
	SNS         SNSKeys `json:"Sns"`
}

// SNSKeys holds the SNS data
type SNSKeys struct {
	MessageID string  `json:"MessageID"`
	TopicArn  string  `json:"TopicArn"`
	Type      string  `json:"Type"`
	TimeStamp string  `json:"Timestamp"`
	Subject   *string `json:"Subject"`
}
