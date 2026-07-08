package notification

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type DestinationType string

const (
	DestinationTypeWebhook  DestinationType = "webhook"
	DestinationTypeRabbitMQ DestinationType = "rabbitmq"
	DestinationTypeKafka    DestinationType = "kafka"
	DestinationTypeAWS      DestinationType = "aws"
)

// eventBridgeARNPrefix identifies notification outbox entries that target the
// AWS EventBridge default event bus for a bucket. These are synthesized when a
// bucket enables EventBridge notifications and use the form
// "eventbridge:<bucket>" rather than a real ARN.
const eventBridgeARNPrefix = "eventbridge:"

type Destination struct {
	Type          DestinationType   `json:"type"`
	URL           string            `json:"url,omitempty"`
	Method        string            `json:"method,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	PayloadFormat PayloadFormat     `json:"payloadFormat,omitempty"`
	Queue         string            `json:"queue,omitempty"`
	Durable       bool              `json:"durable,omitempty"`
	Exchange      string            `json:"exchange,omitempty"`
	ExchangeType  string            `json:"exchangeType,omitempty"`
	RoutingKey    string            `json:"routingKey,omitempty"`
	Brokers       []string          `json:"brokers,omitempty"`
	Topic         string            `json:"topic,omitempty"`
	KeyTemplate   string            `json:"keyTemplate,omitempty"`
	Auth          AuthConfig        `json:"auth,omitempty"`
	TLS           TLSConfig         `json:"tls,omitempty"`
	AWS           AWSConfig         `json:"aws,omitempty"`
}

// AWSConfig carries destination-level overrides for AWS-backed delivery
// (SNS, SQS, Lambda, EventBridge). Empty fields fall back to the ambient AWS
// configuration (environment, shared config, instance role) and, for the
// region, to the region embedded in the destination ARN.
type AWSConfig struct {
	Region          string `json:"region,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"`
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
	// QueueURL, when set, is used directly for SQS delivery instead of
	// resolving the queue URL from the destination ARN.
	QueueURL string `json:"queueUrl,omitempty"`
}

type AuthConfig struct {
	Type     string `json:"type,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type TLSConfig struct {
	Enabled    bool   `json:"enabled,omitempty"`
	CAFile     string `json:"caFile,omitempty"`
	ServerName string `json:"serverName,omitempty"`
	CertFile   string `json:"certFile,omitempty"`
	KeyFile    string `json:"keyFile,omitempty"`
}

type Publisher interface {
	Publish(ctx context.Context, entry *OutboxEntry) error
	Validate(ctx context.Context, arn string, destination Destination) error
}

type RegistryPublisher struct {
	Destinations map[string]Destination
	httpClient   *http.Client
}

func NewRegistryPublisher(destinations map[string]Destination) (*RegistryPublisher, error) {
	return &RegistryPublisher{
		Destinations: destinations,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *RegistryPublisher) Publish(ctx context.Context, entry *OutboxEntry) error {
	// Registry destinations take precedence over AWS fallback delivery.
	destination, ok := p.Destinations[entry.DestinationARN]
	if !ok {
		return publishAWSFallback(ctx, entry, nil)
	}
	switch destination.Type {
	case DestinationTypeWebhook:
		return publishWebhook(ctx, p.httpClient, destination, entry.Payload)
	case DestinationTypeRabbitMQ:
		return publishRabbitMQ(ctx, destination, entry.Payload)
	case DestinationTypeKafka:
		return publishKafka(ctx, destination, entry)
	case DestinationTypeAWS:
		return publishAWSFallback(ctx, entry, &destination)
	default:
		return fmt.Errorf("unsupported notification destination type %q", destination.Type)
	}
}

// publishAWSFallback dispatches an entry to the appropriate AWS service.
// EventBridge-targeted entries (the "eventbridge:<bucket>" convention) are
// delivered to the AWS EventBridge default event bus; everything else is
// routed by ARN service (SNS, SQS, Lambda).
func publishAWSFallback(ctx context.Context, entry *OutboxEntry, destination *Destination) error {
	if strings.HasPrefix(entry.DestinationARN, eventBridgeARNPrefix) {
		return publishEventBridge(ctx, entry, destination)
	}
	return publishAWS(ctx, entry, destination)
}

func (p *RegistryPublisher) Validate(ctx context.Context, arn string, destination Destination) error {
	switch destination.Type {
	case DestinationTypeWebhook:
		if err := validatePayloadFormat(destination.PayloadFormat); err != nil {
			return err
		}
		if destination.URL == "" {
			return errors.New("webhook url is required")
		}
		_, err := http.NewRequestWithContext(ctx, webhookMethod(destination), destination.URL, nil)
		return err
	case DestinationTypeRabbitMQ:
		if err := validatePayloadFormat(destination.PayloadFormat); err != nil {
			return err
		}
		if destination.URL == "" {
			return errors.New("rabbitmq url is required")
		}
		if destination.Queue == "" && destination.Exchange == "" {
			return errors.New("rabbitmq queue or exchange is required")
		}
		return validateRabbitMQ(destination)
	case DestinationTypeKafka:
		if err := validatePayloadFormat(destination.PayloadFormat); err != nil {
			return err
		}
		if len(destination.Brokers) == 0 {
			return errors.New("kafka brokers are required")
		}
		if destination.Topic == "" {
			return errors.New("kafka topic is required")
		}
		return validateKafka(ctx, destination)
	case DestinationTypeAWS:
		return nil
	default:
		return fmt.Errorf("unsupported notification destination type %q", destination.Type)
	}
}

func publishWebhook(ctx context.Context, client *http.Client, destination Destination, payload []byte) error {
	client = clientForDestination(client, destination)
	req, err := http.NewRequestWithContext(ctx, webhookMethod(destination), destination.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range destination.Headers {
		req.Header.Set(key, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func publishRabbitMQ(ctx context.Context, destination Destination, payload []byte) error {
	conn, err := dialRabbitMQ(destination)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	exchange, routingKey := rabbitMQPublishTarget(destination)
	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
		Timestamp:    time.Now().UTC(),
	})
}

func validateRabbitMQ(destination Destination) error {
	conn, err := dialRabbitMQ(destination)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if destination.Queue != "" {
		_, err = ch.QueueDeclarePassive(destination.Queue, destination.Durable, false, false, false, nil)
		return err
	}
	return ch.ExchangeDeclarePassive(destination.Exchange, rabbitMQExchangeType(destination), destination.Durable, false, false, false, nil)
}

func dialRabbitMQ(destination Destination) (*amqp.Connection, error) {
	config := amqp.Config{Heartbeat: 30 * time.Second}
	if destination.Auth.Type == "plain" {
		config.SASL = []amqp.Authentication{&amqp.PlainAuth{Username: destination.Auth.Username, Password: destination.Auth.Password}}
	}
	if destination.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(destination.TLS)
		if err != nil {
			return nil, err
		}
		config.TLSClientConfig = tlsConfig
	}
	return amqp.DialConfig(destination.URL, config)
}

func rabbitMQPublishTarget(destination Destination) (exchange string, routingKey string) {
	if destination.Queue != "" {
		return "", destination.Queue
	}
	return destination.Exchange, destination.RoutingKey
}

func rabbitMQExchangeType(destination Destination) string {
	if destination.ExchangeType == "" {
		return "topic"
	}
	return destination.ExchangeType
}

func publishKafka(ctx context.Context, destination Destination, entry *OutboxEntry) error {
	transport, err := kafkaTransport(destination)
	if err != nil {
		return err
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(destination.Brokers...),
		Topic:        destination.Topic,
		Transport:    transport,
		MaxAttempts:  1,
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()
	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(kafkaMessageKey(destination, entry)),
		Value: entry.Payload,
		Time:  time.Now().UTC(),
	})
}

func kafkaMessageKey(destination Destination, entry *OutboxEntry) string {
	if destination.KeyTemplate == "" {
		return entry.EventName
	}
	values := notificationTemplateValues(entry)
	key := destination.KeyTemplate
	for name, value := range values {
		key = strings.ReplaceAll(key, "{{"+name+"}}", value)
	}
	return key
}

func notificationTemplateValues(entry *OutboxEntry) map[string]string {
	values := map[string]string{"eventName": entry.EventName}
	switch entry.PayloadFormat {
	case PayloadFormatEventBridge:
		var envelope struct {
			Detail struct {
				Bucket struct {
					Name string `json:"name"`
				} `json:"bucket"`
				Object struct {
					Key string `json:"key"`
				} `json:"object"`
			} `json:"detail"`
		}
		if json.Unmarshal(entry.Payload, &envelope) == nil {
			values["bucket"] = envelope.Detail.Bucket.Name
			values["key"] = envelope.Detail.Object.Key
		}
	default:
		var envelope struct {
			Records []struct {
				S3 struct {
					Bucket struct {
						Name string `json:"name"`
					} `json:"bucket"`
					Object struct {
						Key string `json:"key"`
					} `json:"object"`
				} `json:"s3"`
			} `json:"Records"`
		}
		if json.Unmarshal(entry.Payload, &envelope) == nil && len(envelope.Records) > 0 {
			values["bucket"] = envelope.Records[0].S3.Bucket.Name
			values["key"] = envelope.Records[0].S3.Object.Key
		}
	}
	return values
}

func validatePayloadFormat(payloadFormat PayloadFormat) error {
	switch payloadFormat {
	case "", PayloadFormatS3Records, PayloadFormatEventBridge:
		return nil
	default:
		return fmt.Errorf("unsupported notification payload format %q", payloadFormat)
	}
}

func publishAWS(ctx context.Context, entry *OutboxEntry, destination *Destination) error {
	arn, err := parseARN(entry.DestinationARN)
	if err != nil {
		return err
	}
	awsOpts := awsOptionsFor(destination)
	cfg, err := loadAWSConfig(ctx, awsOpts, arn.Region)
	if err != nil {
		return err
	}
	endpoint := awsOpts.Endpoint
	switch arn.Service {
	case "sns":
		client := sns.NewFromConfig(cfg, func(o *sns.Options) { applyBaseEndpoint(&o.BaseEndpoint, endpoint) })
		_, err = client.Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(entry.DestinationARN),
			Message:  aws.String(string(entry.Payload)),
		})
		return err
	case "sqs":
		client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { applyBaseEndpoint(&o.BaseEndpoint, endpoint) })
		queueURL := awsOpts.QueueURL
		if queueURL == "" {
			out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
				QueueName:              aws.String(arn.ResourceName()),
				QueueOwnerAWSAccountId: aws.String(arn.AccountID),
			})
			if err != nil {
				return err
			}
			queueURL = aws.ToString(out.QueueUrl)
		}
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(string(entry.Payload)),
		})
		return err
	case "lambda":
		client := lambda.NewFromConfig(cfg, func(o *lambda.Options) { applyBaseEndpoint(&o.BaseEndpoint, endpoint) })
		_, err = client.Invoke(ctx, &lambda.InvokeInput{
			FunctionName: aws.String(entry.DestinationARN),
			Payload:      entry.Payload,
		})
		return err
	default:
		return fmt.Errorf("unsupported AWS notification destination service %q", arn.Service)
	}
}

// publishEventBridge delivers an EventBridge-formatted payload to the AWS
// EventBridge default event bus.
func publishEventBridge(ctx context.Context, entry *OutboxEntry, destination *Destination) error {
	var envelope struct {
		DetailType string          `json:"detail-type"`
		Source     string          `json:"source"`
		Resources  []string        `json:"resources"`
		Detail     json.RawMessage `json:"detail"`
	}
	if err := json.Unmarshal(entry.Payload, &envelope); err != nil {
		return fmt.Errorf("invalid EventBridge payload: %w", err)
	}
	awsOpts := awsOptionsFor(destination)
	cfg, err := loadAWSConfig(ctx, awsOpts, "")
	if err != nil {
		return err
	}
	client := eventbridge.NewFromConfig(cfg, func(o *eventbridge.Options) { applyBaseEndpoint(&o.BaseEndpoint, awsOpts.Endpoint) })
	detail := "{}"
	if len(envelope.Detail) > 0 {
		detail = string(envelope.Detail)
	}
	_, err = client.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []eventbridgetypes.PutEventsRequestEntry{{
			Source:     aws.String(envelope.Source),
			DetailType: aws.String(envelope.DetailType),
			Detail:     aws.String(detail),
			Resources:  envelope.Resources,
		}},
	})
	return err
}

func awsOptionsFor(destination *Destination) AWSConfig {
	if destination == nil {
		return AWSConfig{}
	}
	return destination.AWS
}

// loadAWSConfig builds an aws.Config honoring destination-level overrides.
// fallbackRegion is used when the destination does not specify a region (for
// example the region embedded in the destination ARN).
func loadAWSConfig(ctx context.Context, opts AWSConfig, fallbackRegion string) (aws.Config, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	region := opts.Region
	if region == "" {
		region = fallbackRegion
	}
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	if opts.AccessKeyID != "" || opts.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(opts.AccessKeyID, opts.SecretAccessKey, opts.SessionToken),
		))
	}
	return awsconfig.LoadDefaultConfig(ctx, loadOpts...)
}

func applyBaseEndpoint(target **string, endpoint string) {
	if endpoint != "" {
		*target = aws.String(endpoint)
	}
}

type arnParts struct {
	Partition string
	Service   string
	Region    string
	AccountID string
	Resource  string
}

func parseARN(value string) (*arnParts, error) {
	parts := strings.SplitN(value, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" {
		return nil, fmt.Errorf("invalid ARN %q", value)
	}
	return &arnParts{
		Partition: parts[1],
		Service:   parts[2],
		Region:    parts[3],
		AccountID: parts[4],
		Resource:  parts[5],
	}, nil
}

func (a *arnParts) ResourceName() string {
	if idx := strings.LastIndexAny(a.Resource, ":/"); idx >= 0 {
		return a.Resource[idx+1:]
	}
	return a.Resource
}

func validateKafka(ctx context.Context, destination Destination) error {
	transport, err := kafkaTransport(destination)
	if err != nil {
		return err
	}
	conn, err := (&kafka.Dialer{
		Timeout:       10 * time.Second,
		TLS:           transport.TLS,
		SASLMechanism: transport.SASL,
	}).DialContext(ctx, "tcp", destination.Brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(destination.Topic)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka topic %q has no partitions", destination.Topic)
	}
	return nil
}

func kafkaTransport(destination Destination) (*kafka.Transport, error) {
	transport := &kafka.Transport{
		Dial: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}
	if destination.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(destination.TLS)
		if err != nil {
			return nil, err
		}
		transport.TLS = tlsConfig
	}
	mechanism, err := kafkaSASLMechanism(destination.Auth)
	if err != nil {
		return nil, err
	}
	transport.SASL = mechanism
	return transport, nil
}

func kafkaSASLMechanism(auth AuthConfig) (sasl.Mechanism, error) {
	switch auth.Type {
	case "", "none":
		return nil, nil
	case "plain", "sasl-plain":
		return plain.Mechanism{Username: auth.Username, Password: auth.Password}, nil
	case "sasl-scram-sha-256":
		return scram.Mechanism(scram.SHA256, auth.Username, auth.Password)
	case "sasl-scram-sha-512":
		return scram.Mechanism(scram.SHA512, auth.Username, auth.Password)
	default:
		return nil, fmt.Errorf("unsupported kafka auth type %q", auth.Type)
	}
}

func webhookMethod(destination Destination) string {
	if destination.Method == "" {
		return http.MethodPost
	}
	return destination.Method
}

func clientForDestination(defaultClient *http.Client, destination Destination) *http.Client {
	if !destination.TLS.Enabled {
		return defaultClient
	}
	tlsConfig, err := buildTLSConfig(destination.TLS)
	if err != nil {
		return defaultClient
	}
	return &http.Client{Timeout: defaultClient.Timeout, Transport: &http.Transport{TLSClientConfig: tlsConfig}}
}

func buildTLSConfig(config TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{ServerName: config.ServerName}
	if config.CAFile != "" {
		ca, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to parse CA file")
		}
		tlsConfig.RootCAs = pool
	}
	if config.CertFile != "" || config.KeyFile != "" {
		if config.CertFile == "" || config.KeyFile == "" {
			return nil, errors.New("both certFile and keyFile are required for mTLS")
		}
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
