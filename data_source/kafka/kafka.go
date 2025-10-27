package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/xuenqlve/common/errors"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	BrokerAddrs []string        `mapstructure:"broker-addrs" toml:"broker-addrs" json:"broker-addrs"`
	CertFile    string          `mapstructure:"cert-file" toml:"cert-file" json:"cert-file"`
	KeyFile     string          `mapstructure:"key-file" toml:"key-file" json:"key-file"`
	CaFile      string          `mapstructure:"ca-file" toml:"ca-file" json:"ca-file"`
	VerifySSL   bool            `mapstructure:"verify-ssl" toml:"verify-ssl" json:"verify-ssl"`
	Mode        string          `mapstructure:"mode" toml:"mode" json:"mode"`
	Producer    *ProducerConfig `mapstructure:"producer" toml:"producer" json:"producer"`
	Consumer    *ConsumerConfig `mapstructure:"consumer" toml:"consumer" json:"consumer"`
	Net         *NetConfig      `mapstructure:"net" toml:"net" json:"net"`

	// 动态分区管理配置
	PartitionRefreshInterval time.Duration `mapstructure:"partition-refresh-interval" json:"partition_refresh_interval"`
	MessageBufferSize        int           `mapstructure:"message-buffer-size" json:"message_buffer_size"`
	ReadTimeout              time.Duration `mapstructure:"read-timeout" json:"read_timeout"`
}

func (c *Config) Init() {
	if c.PartitionRefreshInterval <= 0 {
		c.PartitionRefreshInterval = 30 * time.Second
	}
	if c.MessageBufferSize <= 0 {
		c.MessageBufferSize = 1000
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 5 * time.Second
	}
}

// Connect 返回Kafka配置，与其他数据源保持接口一致
func (c *Config) Connect() (*Config, error) {
	c.Init()
	return c, nil
}

func (c *Config) QueryPartition(topic string) ([]kafka.Partition, error) {
	// 创建临时连接用于查询分区信息
	dialer := &kafka.Dialer{
		Timeout:   c.getDialTimeout(),
		DualStack: true,
	}

	// 配置 TLS
	if c.hasTLSConfig() {
		tlsConfig, err := c.createTlsConfiguration()
		if err != nil {
			return nil, errors.Trace(err)
		}
		dialer.TLS = tlsConfig
	}

	// 配置 SASL
	if c.Net != nil && c.Net.SASL.Enable {
		mechanism := plain.Mechanism{
			Username: c.Net.SASL.User,
			Password: c.Net.SASL.Password,
		}
		dialer.SASLMechanism = mechanism
	}

	// 连接到任意一个broker获取分区信息
	conn, err := dialer.DialContext(context.Background(), "tcp", c.BrokerAddrs[0])
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer conn.Close()

	// 读取分区信息
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return partitions, nil
}

func (c *Config) CreateReader(topic string, partition int) (*kafka.Reader, error) {
	// 创建 Reader 配置
	readerConfig := kafka.ReaderConfig{
		Brokers:   c.BrokerAddrs,
		Topic:     topic,
		Partition: partition, // 默认分区，可以根据需要调整
	}

	// 设置 Consumer 配置
	if c.Consumer != nil {
		readerConfig.GroupID = c.Consumer.GroupID
		readerConfig.StartOffset = c.Consumer.StartOffset
		readerConfig.MinBytes = c.Consumer.MinBytes
		readerConfig.MaxBytes = c.Consumer.MaxBytes
		readerConfig.MaxWait = c.Consumer.MaxWait
	} else {
		// 默认配置
		readerConfig.MinBytes = 10e3 // 10KB
		readerConfig.MaxBytes = 10e6 // 10MB
		readerConfig.MaxWait = 1 * time.Second
	}

	// 设置 Dialer
	dialer := &kafka.Dialer{
		Timeout:   c.getDialTimeout(),
		DualStack: true,
	}

	// 配置 TLS
	if c.hasTLSConfig() {
		tlsConfig, err := c.createTlsConfiguration()
		if err != nil {
			return nil, errors.Trace(err)
		}
		dialer.TLS = tlsConfig
	}

	// 配置 SASL
	if c.Net != nil && c.Net.SASL.Enable {
		mechanism := plain.Mechanism{
			Username: c.Net.SASL.User,
			Password: c.Net.SASL.Password,
		}
		dialer.SASLMechanism = mechanism
	}

	readerConfig.Dialer = dialer

	reader := kafka.NewReader(readerConfig)
	return reader, nil
}

func (c *Config) CreateWriter() (*kafka.Writer, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(c.BrokerAddrs...),
		Balancer: c.getBalancer(),
	}

	// 创建 Transport
	transport := &kafka.Transport{}

	// 设置网络超时
	if c.Net != nil && c.Net.DialTimeout > 0 {
		transport.DialTimeout = c.Net.DialTimeout
	} else {
		transport.DialTimeout = 5 * time.Second
	}

	// 配置 TLS
	if c.hasTLSConfig() {
		tlsConfig, err := c.createTlsConfiguration()
		if err != nil {
			return nil, errors.Trace(err)
		}
		transport.TLS = tlsConfig
	}

	// 配置 SASL
	if c.Net != nil && c.Net.SASL.Enable {
		mechanism := plain.Mechanism{
			Username: c.Net.SASL.User,
			Password: c.Net.SASL.Password,
		}
		transport.SASL = mechanism
	}

	writer.Transport = transport

	// 设置 Producer 配置
	if c.Producer != nil {
		// 设置批次大小
		if c.Producer.BatchSize > 0 {
			writer.BatchSize = c.Producer.BatchSize
		}

		// 设置批次超时
		if c.Producer.BatchTimeout > 0 {
			writer.BatchTimeout = c.Producer.BatchTimeout
		}

		// 设置压缩
		switch c.Producer.Compression {
		case "gzip":
			writer.Compression = kafka.Gzip
		case "snappy":
			writer.Compression = kafka.Snappy
		case "lz4":
			writer.Compression = kafka.Lz4
		case "zstd":
			writer.Compression = kafka.Zstd
		default:
			writer.Compression = kafka.Snappy // 默认使用 Snappy
		}

		// 设置 Flush 配置
		if c.Producer.Flush.Bytes > 0 {
			writer.BatchBytes = int64(c.Producer.Flush.Bytes)
		}

		// 解析 Frequency 并设置 BatchTimeout
		if c.Producer.Flush.Frequency != "" {
			frequency, err := time.ParseDuration(c.Producer.Flush.Frequency)
			if err != nil {
				return nil, errors.Errorf("failed to parse flush frequency: %v", err)
			}
			writer.BatchTimeout = frequency
		}
	} else {
		// 默认配置
		writer.BatchSize = 100
		writer.BatchTimeout = 10 * time.Millisecond
		writer.Compression = kafka.Snappy
	}

	return writer, nil
}

func (c *Config) CreateWriterCustomBalancer(balancer kafka.Balancer) (*kafka.Writer, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(c.BrokerAddrs...),
		Balancer: balancer,
	}

	// 创建 Transport
	transport := &kafka.Transport{}

	// 设置网络超时
	if c.Net != nil && c.Net.DialTimeout > 0 {
		transport.DialTimeout = c.Net.DialTimeout
	} else {
		transport.DialTimeout = 5 * time.Second
	}

	// 配置 TLS
	if c.hasTLSConfig() {
		tlsConfig, err := c.createTlsConfiguration()
		if err != nil {
			return nil, errors.Trace(err)
		}
		transport.TLS = tlsConfig
	}

	// 配置 SASL
	if c.Net != nil && c.Net.SASL.Enable {
		mechanism := plain.Mechanism{
			Username: c.Net.SASL.User,
			Password: c.Net.SASL.Password,
		}
		transport.SASL = mechanism
	}

	writer.Transport = transport

	// 设置 Producer 配置
	if c.Producer != nil {
		// 设置批次大小
		if c.Producer.BatchSize > 0 {
			writer.BatchSize = c.Producer.BatchSize
		}

		// 设置批次超时
		if c.Producer.BatchTimeout > 0 {
			writer.BatchTimeout = c.Producer.BatchTimeout
		}

		// 设置压缩
		switch c.Producer.Compression {
		case "gzip":
			writer.Compression = kafka.Gzip
		case "snappy":
			writer.Compression = kafka.Snappy
		case "lz4":
			writer.Compression = kafka.Lz4
		case "zstd":
			writer.Compression = kafka.Zstd
		default:
			writer.Compression = kafka.Snappy // 默认使用 Snappy
		}

		// 设置 Flush 配置
		if c.Producer.Flush.Bytes > 0 {
			writer.BatchBytes = int64(c.Producer.Flush.Bytes)
		}

		// 解析 Frequency 并设置 BatchTimeout
		if c.Producer.Flush.Frequency != "" {
			frequency, err := time.ParseDuration(c.Producer.Flush.Frequency)
			if err != nil {
				return nil, errors.Errorf("failed to parse flush frequency: %v", err)
			}
			writer.BatchTimeout = frequency
		}
	} else {
		// 默认配置
		writer.BatchSize = 100
		writer.BatchTimeout = 10 * time.Millisecond
		writer.Compression = kafka.Snappy
	}

	return writer, nil
}

func (c *Config) hasTLSConfig() bool {
	return c.CertFile != "" && c.KeyFile != "" && c.CaFile != ""
}

func (c *Config) getDialTimeout() time.Duration {
	if c.Net != nil && c.Net.DialTimeout > 0 {
		return c.Net.DialTimeout
	}
	return 5 * time.Second
}

func (c *Config) createTlsConfiguration() (*tls.Config, error) {
	if !c.hasTLSConfig() {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("LoadX509KeyPair: %v", err)
	}

	caCert, err := os.ReadFile(c.CaFile)
	if err != nil {
		return nil, fmt.Errorf("os.ReadFile: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: !c.VerifySSL, // 注意：这里逻辑相反
	}

	return tlsConfig, nil
}

func (c *Config) UploadBalancer(balancer kafka.Balancer) {
	if c.Producer == nil {
		c.Producer = &ProducerConfig{}
	}
	c.Producer.Balancer = balancer
}

// getBalancer 根据配置返回相应的分区负载均衡器
func (c *Config) getBalancer() kafka.Balancer {
	if c.Producer == nil {
		return &kafka.LeastBytes{} // 默认策略
	}
	if c.Producer.Balancer != nil {
		return c.Producer.Balancer
	}

	switch c.Producer.PartitionStrategy {
	case "round-robin":
		return &kafka.RoundRobin{}
	case "hash":
		return &kafka.Hash{}
	case "manual":
		return nil // 手动分区模式，不使用负载均衡器
	case "least-bytes":
		fallthrough
	default:
		return &kafka.LeastBytes{}
	}
}

type NetConfig struct {
	// SASL based authentication with broker. While there are multiple SASL authentication methods
	// the current implementation is limited to plaintext (SASL/PLAIN) authentication
	SASL SASL `mapstructure:"sasl" toml:"sasl" json:"sasl"`

	// KeepAlive specifies the keep-alive period for an active network connection.
	// If zero, keep-alives are disabled. (default is 0: disabled).
	KeepAlive time.Duration `mapstructure:"keep-alive" toml:"keep-alive" json:"keep-alive"`

	// Timeout settings
	DialTimeout  time.Duration `mapstructure:"dial-timeout" toml:"dial-timeout" json:"dial-timeout"`
	ReadTimeout  time.Duration `mapstructure:"read-timeout" toml:"read-timeout" json:"read-timeout"`
	WriteTimeout time.Duration `mapstructure:"write-timeout" toml:"write-timeout" json:"write-timeout"`
}

type ProducerConfig struct {
	Flush        Flush         `mapstructure:"flush" toml:"flush" json:"flush"`
	Compression  string        `mapstructure:"compression" toml:"compression" json:"compression"`
	BatchSize    int           `mapstructure:"batch-size" toml:"batch-size" json:"batch-size"`
	BatchTimeout time.Duration `mapstructure:"batch-timeout" toml:"batch-timeout" json:"batch-timeout"`
	// 分区策略：可以是 "least-bytes", "round-robin", "hash", "manual"
	PartitionStrategy string `mapstructure:"partition-strategy" toml:"partition-strategy" json:"partition-strategy"`
	Balancer          kafka.Balancer
}

type ConsumerConfig struct {
	GroupID     string        `mapstructure:"group-id" toml:"group-id" json:"group-id"`
	StartOffset int64         `mapstructure:"start-offset" toml:"start-offset" json:"start-offset"`
	MinBytes    int           `mapstructure:"min-bytes" toml:"min-bytes" json:"min-bytes"`
	MaxBytes    int           `mapstructure:"max-bytes" toml:"max-bytes" json:"max-bytes"`
	MaxWait     time.Duration `mapstructure:"max-wait" toml:"max-wait" json:"max-wait"`
}

type Fetch struct {
	Min     int32 `toml:"min" json:"min"`
	Default int32 `toml:"default" json:"default"`
	Max     int32 `toml:"max" json:"max"`
}

type Flush struct {
	Bytes       int    `mapstructure:"bytes" toml:"bytes" json:"bytes"`
	Messages    int    `mapstructure:"messages" toml:"messages" json:"messages"`
	Frequency   string `mapstructure:"frequency" toml:"frequency" json:"frequency"`
	MaxMessages int    `mapstructure:"max-messages" toml:"max-messages" json:"max-messages"`
}

type SASL struct {
	Enable   bool   `mapstructure:"enable" toml:"enable" json:"enable"`
	User     string `mapstructure:"user" toml:"user" json:"user"`
	Password string `mapstructure:"password" toml:"password" json:"password"`
}

func NewWriterClient(config Config, balancer kafka.Balancer) (*WriterClient, error) {
	writer, err := config.CreateWriterCustomBalancer(balancer)
	if err != nil {
		return nil, err
	}

	return &WriterClient{
		Writer: writer,
		Config: config,
	}, nil
}

type WriterClient struct {
	Writer *kafka.Writer
	Config Config
}

// WriteMessage 写入单条消息
func (c *WriterClient) WriteMessage(ctx context.Context, topic string, key, value []byte) error {
	if c.Writer == nil {
		return errors.New("kafka-docker writer is not initialized")
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	return c.Writer.WriteMessages(ctx, msg)
}

// WriteMessages 批量写入消息
func (c *WriterClient) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	if c.Writer == nil {
		return errors.New("kafka-docker writer is not initialized")
	}
	return c.Writer.WriteMessages(ctx, messages...)
}

func (c *WriterClient) Close() error {
	var err error
	if c.Writer != nil {
		if closeErr := c.Writer.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}

// Stats 获取统计信息
func (c *WriterClient) Stats() kafka.WriterStats {
	if c.Writer != nil {
		return c.Writer.Stats()
	}
	return kafka.WriterStats{}
}

type ReaderClient struct {
	Reader *kafka.Reader
	Config *Config
}

func NewReaderClient(config *Config, topic string, partition int) (*ReaderClient, error) {
	reader, err := config.CreateReader(topic, partition)
	if err != nil {
		return nil, err
	}

	return &ReaderClient{
		Reader: reader,
		Config: config,
	}, nil
}

func (r *ReaderClient) Close() error {
	return r.Reader.Close()
}
