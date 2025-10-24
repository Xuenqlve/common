package oplog

//var oplogMu sync.Mutex
//
//func FetchOplogNewestTimestamp(ctx context.Context, dataSource string) (*PositionValue, error) {
//	client, err := MongoDBStreamConn(ctx, dataSource, DefaultReaderFetchBatchSize, nil)
//	if err != nil {
//		return nil, err
//	}
//	defer func() {
//		if err = client.Close(ctx); err != nil {
//			log.Errorf("MongoDB StreamConn client close err:%v", err)
//		}
//	}()
//
//	client.TryNext(ctx)
//	token := client.ResumeToken()
//	pos := Position{
//		Token:     token,
//		Timestamp: InitCheckpoint,
//	}
//	return pos.InitPositionValue(), nil
//}

type Position struct {
	Token     interface{} `mapstructure:"token" json:"token"`
	Timestamp int64       `mapstructure:"timestamp" json:"timestamp"`
}

func (p Position) Check() bool {
	if p.Token != nil {
		return true
	}
	if p.Timestamp != 0 {
		return true
	}
	return false
}

//func NewOplogPositionValue() *PositionValue {
//	return &PositionValue{}
//}
//type PositionValue struct {
//	mu              sync.Mutex
//	StartPosition   Position `json:"start_position"`
//	CurrentPosition Position `json:"current_position"`
//}
//
//func (b *PositionValue) Copy(v *PositionValue) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.CurrentPosition = v.CurrentPosition
//	b.StartPosition = v.StartPosition
//}
//
//func (b *PositionValue) SetPosition(startTimestamp, currentTimestamp Position) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.StartPosition = startTimestamp
//	b.CurrentPosition = currentTimestamp
//}
//
//func (b *PositionValue) GetCurrentPosition() Position {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	return b.CurrentPosition
//}
//
//func (b *PositionValue) PushCurrentPosition(pos Position) {
//	b.mu.Lock()
//	defer b.mu.Unlock()
//	b.CurrentPosition = pos
//}
//
//func (b *PositionValue) MergeUpload(pos position.Position) (position.Position, error) {
//	posValue, ok := pos.Value.(Position)
//	if !ok {
//		return position.Position{}, errors.Errorf("invalid position type: %T", pos.Value)
//	}
//	b.PushCurrentPosition(posValue)
//	return position.Position{
//		Meta:        pos.Meta,
//		ForceCommit: false,
//		Value:       b,
//	}, nil
//}
//
//func (b *PositionValue) ValueInterface() position.ValueInterface {
//	return position.ValueInterface{
//		ValueEncoder: EncodeBinlogPositionValue,
//		ValueDecoder: DecodeBinlogPositionValue,
//	}
//}
//
//func EncodeBinlogPositionValue(v any) (string, error) {
//	value, ok := v.(*PositionValue)
//	if !ok {
//		return "", errors.Errorf("invalid position value type: %v", reflect.TypeOf(v))
//	}
//	oplogMu.Lock()
//	defer oplogMu.Unlock()
//	data, err := json.Marshal(value)
//	if err != nil {
//		return "", err
//	}
//	return string(data), nil
//}
//
//func DecodeBinlogPositionValue(s string) (any, error) {
//	value := PositionValue{}
//	if err := json.Unmarshal([]byte(s), &value); err != nil {
//		return nil, errors.Trace(err)
//	}
//	return &value, nil
//}
