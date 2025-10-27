package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/xuenqlve/common/errors"
	"github.com/xuenqlve/common/log"
)

type Config struct {
	Host     string `yaml:"host" json:"host" mapstructure:"host"`
	Port     int    `yaml:"port" json:"port" mapstructure:"port"`
	User     string `yaml:"user" json:"user" mapstructure:"user"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`

	Timeout        string `yaml:"timeout" json:"timeout" mapstructure:"timeout"`
	MaxConnections int    `yaml:"max_connections" json:"max_connections" mapstructure:"max_connections"`
	Secure         bool   `yaml:"secure" json:"secure" mapstructure:"secure"`
	SkipVerify     bool   `yaml:"skip-verify" json:"skip-verify" mapstructure:"skip-verify"`
	TLSKey         string `yaml:"tls-key" json:"tls-key" mapstructure:"tls-key"`
	TLSCert        string `yaml:"tls-cert" json:"tls-cert" mapstructure:"tls-cert"`
	TLSCa          string `yaml:"tls-ca" json:"tls-ca" mapstructure:"tls-ca"`

	Debug bool `yaml:"debug" json:"debug" mapstructure:"debug"`
}

func (ch *Config) ValidateAndSetDefault() (err error) {
	if ch.Host == "" {
		return fmt.Errorf("clickhouse host is required")
	}
	if ch.Port == 0 {
		return fmt.Errorf("clickhouse port is required")
	}
	if ch.User == "" {
		return fmt.Errorf("clickhouse user is required")
	}
	if ch.Password == "" {
		return fmt.Errorf("clickhouse password is required")
	}
	if ch.Timeout == "" {
		ch.Timeout = "30m"
	}
	downloadConcurrency := uint8(1)
	if runtime.NumCPU() > 1 {
		downloadConcurrency = uint8(runtime.NumCPU() / 2)
	}
	if downloadConcurrency < 1 {
		downloadConcurrency = 1
	}
	ch.MaxConnections = int(downloadConcurrency)
	return nil
}

func (ch *Config) Connect() (conn driver.Conn, err error) {
	if err = ch.ValidateAndSetDefault(); err != nil {
		return
	}
	timeout, err := time.ParseDuration(ch.Timeout)
	if err != nil {
		return
	}
	opt := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", ch.Host, ch.Port)},
		Auth: clickhouse.Auth{
			Database: ch.Database,
			Username: ch.User,
			Password: ch.Password,
		},
		Settings: clickhouse.Settings{
			"connect_timeout":      int(timeout.Seconds()),
			"receive_timeout":      int(timeout.Seconds()),
			"send_timeout":         int(timeout.Seconds()),
			"http_send_timeout":    300,
			"http_receive_timeout": 300,
		},
		MaxOpenConns:    ch.MaxConnections,
		ConnMaxLifetime: 0,
		MaxIdleConns:    0,
		DialTimeout:     timeout,
		ReadTimeout:     timeout,
	}
	if ch.Debug {
		opt.Debug = true
	}
	if ch.Secure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: ch.SkipVerify,
		}
		if ch.TLSKey != "" || ch.TLSCert != "" || ch.TLSCa != "" {
			if ch.TLSCert != "" || ch.TLSKey != "" {
				cert, err := tls.LoadX509KeyPair(ch.TLSCert, ch.TLSKey)
				if err != nil {
					log.Errorf("tls.LoadX509KeyPair error: %v", err)
					return nil, err
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
			if ch.TLSCa != "" {
				caCert, err := os.ReadFile(ch.TLSCa)
				if err != nil {
					log.Errorf("read `tls_ca` file %s return error: %v ", ch.TLSCa, err)
					return nil, err
				}
				caCertPool := x509.NewCertPool()
				if caCertPool.AppendCertsFromPEM(caCert) != true {
					log.Errorf("AppendCertsFromPEM %s return false", ch.TLSCa)
					return nil, fmt.Errorf("AppendCertsFromPEM %s return false", ch.TLSCa)
				}
				tlsConfig.RootCAs = caCertPool
			}
		}
		opt.TLS = tlsConfig
	}

	conn, err = clickhouse.Open(opt)
	if err != nil {
		log.Errorf("Open ClickHouse error: %v", err)
		err = errors.Errorf("failed to connect ClickHouse open error:%v", err)
		return
	}
	if err = conn.Ping(context.Background()); err != nil {
		err = errors.Errorf("failed to connect ClickHouse ping error:%v", err)
		return
	}
	return conn, nil
}
