package notification

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

type Destination struct {
	Type          DestinationType   `json:"type"`
	URL           string            `json:"url,omitempty"`
	Method        string            `json:"method,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	PayloadFormat PayloadFormat     `json:"payloadFormat,omitempty"`
	Queue         string            `json:"queue,omitempty"`
	Durable       bool              `json:"durable,omitempty"`
	Exchange      string            `json:"exchange,omitempty"`
	RoutingKey    string            `json:"routingKey,omitempty"`
	Brokers       []string          `json:"brokers,omitempty"`
	Topic         string            `json:"topic,omitempty"`
	KeyTemplate   string            `json:"keyTemplate,omitempty"`
	Auth          AuthConfig        `json:"auth,omitempty"`
	TLS           TLSConfig         `json:"tls,omitempty"`
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
	destination, ok := p.Destinations[entry.DestinationARN]
	if !ok {
		return publishAWS(ctx, entry)
	}
	switch destination.Type {
	case DestinationTypeWebhook:
		return publishWebhook(ctx, p.httpClient, destination, entry.Payload)
	case DestinationTypeRabbitMQ:
		return publishRabbitMQ(ctx, destination, entry.Payload)
	case DestinationTypeKafka:
		return publishKafka(ctx, destination, entry)
	case DestinationTypeAWS:
		return publishAWS(ctx, entry)
	default:
		return fmt.Errorf("unsupported notification destination type %q", destination.Type)
	}
}

func (p *RegistryPublisher) Validate(ctx context.Context, arn string, destination Destination) error {
	switch destination.Type {
	case DestinationTypeWebhook:
		if destination.URL == "" {
			return errors.New("webhook url is required")
		}
		_, err := http.NewRequestWithContext(ctx, webhookMethod(destination), destination.URL, nil)
		return err
	case DestinationTypeRabbitMQ:
		if destination.URL == "" {
			return errors.New("rabbitmq url is required")
		}
		if destination.Queue == "" && destination.Exchange == "" {
			return errors.New("rabbitmq queue or exchange is required")
		}
		return validateRabbitMQ(destination)
	case DestinationTypeKafka:
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
	return ch.ExchangeDeclarePassive(destination.Exchange, "topic", destination.Durable, false, false, false, nil)
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
		Key:   []byte(entry.EventName),
		Value: entry.Payload,
		Time:  time.Now().UTC(),
	})
}

func publishAWS(ctx context.Context, entry *OutboxEntry) error {
	arn, err := parseARN(entry.DestinationARN)
	if err != nil {
		return err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(arn.Region))
	if err != nil {
		return err
	}
	switch arn.Service {
	case "sns":
		_, err = sns.NewFromConfig(cfg).Publish(ctx, &sns.PublishInput{
			TopicArn: aws.String(entry.DestinationARN),
			Message:  aws.String(string(entry.Payload)),
		})
		return err
	case "sqs":
		client := sqs.NewFromConfig(cfg)
		queueURL, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName:              aws.String(arn.ResourceName()),
			QueueOwnerAWSAccountId: aws.String(arn.AccountID),
		})
		if err != nil {
			return err
		}
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    queueURL.QueueUrl,
			MessageBody: aws.String(string(entry.Payload)),
		})
		return err
	case "lambda":
		_, err = lambda.NewFromConfig(cfg).Invoke(ctx, &lambda.InvokeInput{
			FunctionName: aws.String(entry.DestinationARN),
			Payload:      entry.Payload,
		})
		return err
	default:
		return fmt.Errorf("unsupported AWS notification destination service %q", arn.Service)
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
