package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xuenqlve/common/errors"
	"net/url"
	"time"
)

const DefaultMySQLVersion = "5.7"

type Config struct {
	Host     string `toml:"host" json:"host" mapstructure:"host"`
	Location string `toml:"location" json:"location" mapstructure:"location"`
	Username string `toml:"username" json:"username" mapstructure:"username"`
	Password string `toml:"password" json:"password" mapstructure:"password"`
	Port     int    `toml:"port" json:"port" mapstructure:"port"`
	Schema   string `toml:"schema_store" json:"schema_store" mapstructure:"schema_store"`
	// Timeout for establishing connections, aka dial timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	Timeout string `toml:"timeout" json:"timeout" mapstructure:"timeout"`
	// I/O read timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	ReadTimeout string `toml:"read-timeout" json:"read-timeout" mapstructure:"read-timeout"`

	// I/O write timeout.
	// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
	WriteTimeout string `toml:"write-timeout" json:"write-timeout" mapstructure:"write-timeout"`

	MaxIdle                int           `toml:"max-idle" json:"max-idle" mapstructure:"max-idle"`
	MaxOpen                int           `toml:"max-open" json:"max-open" mapstructure:"max-open"`
	MaxLifeTimeDurationStr string        `toml:"max-life-time-duration" json:"max-life-time-duration" mapstructure:"max-life-time-duration"`
	MaxLifeTimeDuration    time.Duration `toml:"-" json:"-" mapstructure:"-"`
	MySQLVersion           string        `toml:"mysql-version" json:"mysql-version" mapstructure:"mysql-version"`
}

func (c *Config) ValidateAndSetDefault() error {
	// Sets the location for time.Time values (when using parseTime=true). "Local" sets the system's location. See time.LoadLocation for details.
	// Note that this sets the location for time.Time values but does not change MySQL's time_zone setting.
	// For that see the time_zone system variable, which can also be set as a DSN parameter.
	if c.Location == "" {
		c.Location = time.Local.String()
	}

	if c.MaxOpen == 0 {
		// for mysql, 20 is the default pool size in many conn pool implementations
		// such as https://github.com/alibaba/druid/wiki/DruidDataSource%E9%85%8D%E7%BD%AE#1-%E9%80%9A%E7%94%A8%E9%85%8D%E7%BD%AE
		c.MaxOpen = 20
	}

	if c.MaxIdle == 0 {
		c.MaxIdle = c.MaxOpen
	}

	var err error
	if c.MaxLifeTimeDurationStr == "" {
		c.MaxLifeTimeDurationStr = "1h"
		c.MaxLifeTimeDuration = time.Hour
	} else {
		c.MaxLifeTimeDuration, err = time.ParseDuration(c.MaxLifeTimeDurationStr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if c.Timeout == "" {
		c.Timeout = "5s"
	}

	if c.ReadTimeout == "" {
		c.ReadTimeout = "5s"
	}

	if c.WriteTimeout == "" {
		c.WriteTimeout = "5s"
	}

	if c.MySQLVersion == "" {
		c.MySQLVersion = DefaultMySQLVersion
	}
	return nil
}

func (c *Config) Connect() (*sql.DB, error) {
	if err := c.ValidateAndSetDefault(); err != nil {
		return nil, errors.Trace(err)
	}
	dbDSN := fmt.Sprintf(`%s:%s@tcp(%s:%d)/%s?interpolateParams=true&timeout=%s&readTimeout=%s&writeTimeout=%s&parseTime=false&collation=utf8mb4_general_ci&charset=utf8mb4&multiStatements=true`,
		c.Username, c.Password, c.Host, c.Port, url.QueryEscape(c.Schema), c.Timeout, c.ReadTimeout, c.WriteTimeout)
	if c.Location != "" {
		dbDSN += "&loc=" + url.QueryEscape(c.Location)
	}
	if c.MySQLVersion == DefaultMySQLVersion {
		dbDSN += "&transaction_isolation=" + url.QueryEscape("'read-committed'")
	}

	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = db.Ping()
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxOpenConns(c.MaxOpen)
	db.SetMaxIdleConns(c.MaxIdle)
	db.SetConnMaxLifetime(c.MaxLifeTimeDuration)

	return db, nil
}
