package ebus

import (
	"fmt"
	"strings"
	"time"
)

const (
	ContentTypeJson = "application/json"
)

// Event 事件接口 (所有事件都需要实现此接口)
type Event interface {

	// Metadata 获取事件元数据
	Metadata() *Metadata

	// Validate 验证事件是否有效
	Validate() error
}

// Envelope 表示事件信封
type Envelope struct {
	Metadata *Metadata `json:"metadata"` // 事件元数据
	Payload  []byte    `json:"payload"`  // 事件负载
}

// SchemaVersion 表示事件模型版本
type SchemaVersion string

func (ver SchemaVersion) String() string {
	return string(ver)
}

func (ver SchemaVersion) Normalize() SchemaVersion {
	str := string(ver)
	str = strings.TrimSpace(str)
	str = strings.ToLower(str)
	return SchemaVersion(str)
}

func (ver SchemaVersion) IsEmpty() bool {
	str := ver.Normalize().String()
	return len(str) == 0
}

// EventSource 表示事件来源
type EventSource string

func (es EventSource) String() string {
	return string(es)
}

func (es EventSource) Normalize() EventSource {
	str := string(es)
	str = strings.TrimSpace(str)
	str = strings.ToLower(str)
	return EventSource(str)
}

func (es EventSource) IsEmpty() bool {
	str := es.Normalize().String()
	return len(str) == 0
}

// EventType 表示事件类型
type EventType string

func (et EventType) String() string {
	return string(et)
}

func (et EventType) Normalize() EventType {
	str := string(et)
	str = strings.TrimSpace(str)
	str = strings.ToLower(str)
	return EventType(str)
}

func (et EventType) IsEmpty() bool {
	str := et.Normalize().String()
	return len(str) == 0
}

// Metadata 表示事件元数据
type Metadata struct {
	SchemaVersion SchemaVersion `json:"schemaVersion"` // 模型版本
	EventId       string        `json:"eventId"`       // 事件ID, 全局唯一
	EventSource   EventSource   `json:"eventSource"`   // 事件来源
	EventType     EventType     `json:"eventType"`     // 事件类型
	EventTime     int64         `json:"eventTime"`     // 事件时间, Unix时间戳, 单位秒
}

func (meta *Metadata) Normalize() {
	meta.SchemaVersion = meta.SchemaVersion.Normalize()
	meta.EventId = strings.TrimSpace(meta.EventId)
	meta.EventSource = meta.EventSource.Normalize()
	meta.EventType = meta.EventType.Normalize()
}

func (meta *Metadata) Validate() error {
	meta.Normalize()

	if meta.SchemaVersion.IsEmpty() {
		return fmt.Errorf("ebus: 模型版本不能为空")
	}

	if len(meta.EventId) == 0 {
		return fmt.Errorf("ebus: 事件ID不能为空")
	}

	if meta.EventSource.IsEmpty() {
		return fmt.Errorf("ebus: 事件来源不能为空")
	}

	if meta.EventType.IsEmpty() {
		return fmt.Errorf("ebus: 事件类型不能为空")
	}

	if meta.EventTime <= 0 {
		return fmt.Errorf("ebus: 事件时间必须大于0")
	}

	// 如果这条消息来自未来的时间
	// 说明发送者的时钟比接收者快
	// 允许最大300秒的时钟漂移
	const maxFutureSecs = 300
	if meta.EventTime > (time.Now().Unix() + maxFutureSecs) {
		return fmt.Errorf("ebus: 事件时间超出允许范围(>%d秒)", maxFutureSecs)
	}

	return nil
}
