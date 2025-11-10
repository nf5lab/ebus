package ebus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nf5lab/broker"
)

// Publisher 发布者接口
type Publisher interface {

	// Publish 发布事件
	Publish(ctx context.Context, topic string, event Event) error

	// Close 关闭发布者 (不会执行任何操作)
	//
	// Deprecated: 此方法已废弃, 将在未来版本中移除
	// ebus.Publisher 不应该管理底层资源的生命周期
	Close() error
}

type publisher struct {
	inner broker.Publisher
}

// NewPublisher 创建发布者
func NewPublisher(brokerPublisher broker.Publisher) Publisher {
	return &publisher{
		inner: brokerPublisher,
	}
}

// Publish 发布事件
func (pub *publisher) Publish(ctx context.Context, topic string, event Event) error {
	topic = strings.TrimSpace(topic)
	if len(topic) == 0 {
		return fmt.Errorf("ebus: 主题不能为空")
	}

	if event == nil {
		return fmt.Errorf("ebus: 事件不能为空")
	}

	if err := event.Validate(); err != nil {
		return fmt.Errorf("ebus: 事件无效: %w", err)
	}

	metadata := event.Metadata()
	if metadata == nil {
		return fmt.Errorf("ebus: 事件元数据不能为空")
	}

	if err := metadata.Validate(); err != nil {
		return fmt.Errorf("ebus: 事件(%s)元数据无效: %w", metadata.EventId, err)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("ebus: 事件(%s)编码失败: %w", metadata.EventId, err)
	}

	// 构建信封
	envelope := &Envelope{
		Metadata: metadata,
		Payload:  payload,
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("ebus: 事件信封(%s)编码失败: %w", metadata.EventId, err)
	}

	// 创建消息
	message := &broker.Message{
		Id:          metadata.EventId,
		Headers:     make(map[string]any),
		Body:        data,
		ContentType: ContentTypeJson,
	}

	// 设置消息头
	for key, value := range metadataToHeaders(metadata) {
		message.AddHeader(key, value)
	}

	// 发布消息
	if err := pub.inner.Publish(ctx, topic, message); err != nil {
		return fmt.Errorf("ebus: 事件(%s)发布失败: %w", metadata.EventId, err)
	}

	return nil
}

// Close 关闭发布者 (不会执行任何操作)
//
// Deprecated: 此方法已废弃, 将在未来版本中移除
// ebus.Publisher 不应该管理底层资源的生命周期
func (pub *publisher) Close() error {
	return nil
}
