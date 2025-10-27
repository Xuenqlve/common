package mongodb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/xuenqlve/common/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	ReadConcernDefault      = ""
	ReadConcernLocal        = "local"
	ReadConcernAvailable    = "available" // for >= 3.6
	ReadConcernMajority     = "majority"
	ReadConcernLinearizable = "linearizable"

	WriteConcernDefault  = ""
	WriteConcernMajority = "majority"

	VarMongoConnectModePrimary            = "primary"
	VarMongoConnectModePrimaryPreferred   = "primaryPreferred"
	VarMongoConnectModeSecondaryPreferred = "secondaryPreferred"
	VarMongoConnectModeSecondary          = "secondary"
	VarMongoConnectModeNearset            = "nearest"
	VarMongoConnectModeStandalone         = "standalone"
)

type Config struct {
	Host         []string `mapstructure:"urls" json:"urls" toml:"urls" yaml:"urls"`
	ReplicaSet   string   `mapstructure:"replica-set" json:"replica-set" toml:"replica-set" yaml:"replica-set"`
	Username     string   `mapstructure:"username" json:"username" toml:"username" yaml:"username"`
	Password     string   `mapstructure:"password" json:"password" toml:"password" yaml:"password"`
	AuthSource   string   `mapstructure:"auth-source" json:"auth-source" toml:"auth-source" yaml:"auth-source"`
	ConnectMode  string   `mapstructure:"connect-mode" json:"connect-mode" toml:"connect-mode" yaml:"connect-mode"`
	SslRootFile  string   `mapstructure:"ssl-root-file" json:"ssl-root-file" toml:"ssl-root-file" yaml:"ssl-root-file"`
	ReadConcern  string   `mapstructure:"read-concern" json:"read-concern" toml:"read-concern" yaml:"read-concern"`
	WriteConcern any      `mapstructure:"write-concern" json:"write-concern" toml:"write-concern" yaml:"write-concern"`
	Timeout      bool     `mapstructure:"timeout" json:"timeout" toml:"timeout" yaml:"timeout"`
}

func (c *Config) ValidateAndSetDefault() error {
	if len(c.Host) == 0 {
		return fmt.Errorf("mongo_schema config urls is empty")
	}
	if c.Username == "" {
		return fmt.Errorf("mongo_schema config username is empty")
	}
	if c.Password == "" {
		return fmt.Errorf("mongo_schema config password is empty")
	}
	if c.AuthSource == "" {
		c.AuthSource = "admin"
	}
	return nil
}

func (c *Config) makeURL() string {
	hosts := strings.Join(c.Host, ",")
	url := fmt.Sprintf("mongodb://%s", hosts)
	if c.ReplicaSet != "" {
		url = fmt.Sprintf("%s/?replicaSet=%s", url, c.ReplicaSet)
	}
	return url
}

func (c *Config) Connect() (*mongo.Client, error) {
	if err := c.ValidateAndSetDefault(); err != nil {
		return nil, errors.Trace(err)
	}
	clientOps := options.Client().ApplyURI(c.makeURL()).SetAuth(options.Credential{
		AuthSource: c.AuthSource,
		Username:   c.Username,
		Password:   c.Password,
	})
	if c.SslRootFile != "" {
		tlsConfig := new(tls.Config)

		err := addCACertFromFile(tlsConfig, c.SslRootFile)
		if err != nil {
			return nil, fmt.Errorf("load rootCaFile[%v] failed: %v", c.SslRootFile, err)
		}

		// not check hostname
		tlsConfig.InsecureSkipVerify = true

		clientOps.SetTLSConfig(tlsConfig)
	}

	switch c.ReadConcern {
	case ReadConcernDefault:
	default:
		clientOps.SetReadConcern(&readconcern.ReadConcern{Level: c.ReadConcern})
	}

	switch c.WriteConcern {
	case WriteConcernDefault:
	default:
		clientOps.SetWriteConcern(&writeconcern.WriteConcern{W: c.WriteConcern})
	}

	readPreference := &readpref.ReadPref{}
	switch c.ConnectMode {
	case VarMongoConnectModePrimary:
		readPreference = readpref.Primary()
	case VarMongoConnectModePrimaryPreferred:
		readPreference = readpref.PrimaryPreferred()
	case VarMongoConnectModeSecondaryPreferred:
		readPreference = readpref.SecondaryPreferred()
	case VarMongoConnectModeSecondary:
		readPreference = readpref.Secondary()
	case VarMongoConnectModeNearset:
		readPreference = readpref.Nearest()
	default:
		readPreference = readpref.Primary()
	}
	clientOps.SetReadPreference(readPreference)

	if !c.Timeout {
		clientOps.SetConnectTimeout(0)
	} else {
		clientOps.SetConnectTimeout(20 * time.Minute)
	}
	ctx := context.Background()

	client, err := mongo.Connect(ctx, clientOps)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = client.Ping(ctx, clientOps.ReadPreference); err != nil {
		return nil, errors.Trace(err)
	}

	return client, nil
}

func addCACertFromFile(cfg *tls.Config, file string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	certBytes, err := loadCert(data)
	if err != nil {
		return err
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return err
	}

	if cfg.RootCAs == nil {
		cfg.RootCAs = x509.NewCertPool()
	}

	cfg.RootCAs.AddCert(cert)

	return nil
}

func loadCert(data []byte) ([]byte, error) {
	var certBlock *pem.Block

	for certBlock == nil {
		if data == nil || len(data) == 0 {
			return nil, fmt.Errorf(".pem file must have both a CERTIFICATE and an RSA PRIVATE KEY section")
		}

		block, rest := pem.Decode(data)
		if block == nil {
			return nil, fmt.Errorf("invalid .pem file")
		}

		switch block.Type {
		case "CERTIFICATE":
			certBlock = block
		}

		data = rest
	}

	return certBlock.Bytes, nil
}

type connType string

const (
	WriteConnType connType = "write"
	ReadConnType  connType = "read"
)
