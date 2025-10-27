package redis

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xuenqlve/common/data_source/redis/proto"
	"github.com/xuenqlve/common/log"
)

const (
	Nil = redis.Nil
)

type Redis struct {
	reader      *bufio.Reader
	writer      *bufio.Writer
	protoReader *proto.Reader
	protoWriter *proto.Writer
}

func CreateRedisConnection(address string, username string, password string, isTls bool) (*Redis, error) {
	r := new(Redis)
	var conn net.Conn
	var dialer net.Dialer
	var err error
	dialer.Timeout = 3 * time.Second
	if isTls {
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	} else {
		conn, err = dialer.Dial("tcp", address)
	}
	if err != nil {
		return nil, err
	}

	r.reader = bufio.NewReader(conn)
	r.writer = bufio.NewWriter(conn)
	r.protoReader = proto.NewReader(r.reader)
	r.protoWriter = proto.NewWriter(r.writer)

	// auth
	if password != "" {
		var reply string
		if username != "" {
			reply = r.DoWithStringReply("auth", username, password)
		} else {
			reply = r.DoWithStringReply("auth", password)
		}
		if reply != "OK" {
			err = fmt.Errorf("auth failed with reply: %s", reply)
			return nil, err
		}
		log.Infof("auth successful. address=[%s]", address)
	} else {
		log.Infof("no password. address=[%s]", address)
	}

	if err = r.Ping(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Redis) Ping() error {
	reply := r.DoWithStringReply("ping")
	if reply != "PONG" {
		return fmt.Errorf("ping failed with reply: " + reply)
	}
	return nil
}

func (r *Redis) DoWithStringReply(args ...string) string {
	r.Send(args...)

	replyInterface, err := r.Receive()
	if err != nil {
		log.PanicError(err)
	}
	reply := replyInterface.(string)
	return reply
}

func (r *Redis) Send(args ...string) {
	argsInterface := make([]interface{}, len(args))
	for inx, item := range args {
		argsInterface[inx] = item
	}
	err := r.protoWriter.WriteArgs(argsInterface)
	if err != nil {
		log.PanicError(err)
	}
	r.flush()
}

func (r *Redis) SendBytes(buf []byte) {
	_, err := r.writer.Write(buf)
	if err != nil {
		log.PanicError(err)
	}
	r.flush()
}

func (r *Redis) flush() {
	err := r.writer.Flush()
	if err != nil {
		log.PanicError(err)
	}
}

func (r *Redis) Process(buf []byte) (interface{}, error) {
	_, err := r.writer.Write(buf)
	if err != nil {
		return nil, err
	}
	if err = r.writer.Flush(); err != nil {
		return nil, err
	}
	return r.protoReader.ReadReply()
}

func (r *Redis) Receive() (interface{}, error) {
	return r.protoReader.ReadReply()
}

func (r *Redis) BufioReader() *bufio.Reader {
	return r.reader
}

func (r *Redis) SetBufioReader(rd *bufio.Reader) {
	r.reader = rd
	r.protoReader = proto.NewReader(r.reader)
}

/* Commands */

func (r *Redis) Scan(cursor uint64) (newCursor uint64, keys []string) {
	r.Send("scan", strconv.FormatUint(cursor, 10), "count", "2048")
	reply, err := r.Receive()
	if err != nil {
		log.PanicError(err)
	}

	array := reply.([]interface{})
	if len(array) != 2 {
		log.Panicf("scan return length error. ret=%v", reply)
	}

	// cursor
	newCursor, err = strconv.ParseUint(array[0].(string), 10, 64)
	if err != nil {
		log.PanicError(err)
	}
	// keys
	keys = make([]string, 0)
	for _, item := range array[1].([]interface{}) {
		keys = append(keys, item.(string))
	}
	return
}

func (r *Redis) Close() {
}
