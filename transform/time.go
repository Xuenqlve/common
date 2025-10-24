package transform

import "time"

func ConvertUint32ToTime(timestamp uint32) time.Time {
	return time.Unix(int64(timestamp), 0)
}
