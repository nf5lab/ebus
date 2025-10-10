package ebus

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/nf5lab/broker"
)

// EventHandler 事件处理函数
//
// - 处理成功返回 nil
// - 处理失败返回 error
type EventHandler func(ctx context.Context, topic string, event Event) error

// Subscriber 订阅者接口
type Subscriber interface {

	// Subscribe 订阅事件
	Subscribe(ctx context.Context, topic string, group string, handler EventHandler) (string, error)

	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, subscriptionId string) error

	// Close 关闭订阅者
	Close() error
}

type subscriber struct {
	inner broker.Subscriber
}

// NewSubscriber 创建订阅者
func NewSubscriber(brokerSubscriber broker.Subscriber) Subscriber {
	return &subscriber{
		inner: brokerSubscriber,
	}
}

// decodeEvent 解码事件
func (sub *subscriber) decodeEvent(data []byte) (Event, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("ebus: 事件数据为空")
	}

	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("ebus: 事件信封解码失败: %w", err)
	}

	metadata := envelope.Metadata
	if metadata == nil {
		return nil, fmt.Errorf("ebus: 事件信封元数据为空")
	}

	if err := metadata.Validate(); err != nil {
		return nil, fmt.Errorf("ebus: 事件信封元数据无效: %w", err)
	}

	if len(envelope.Payload) == 0 {
		return nil, fmt.Errorf("ebus: 事件信封负载为空")
	}

	factory, err := GetEventFactory(metadata.SchemaVersion, metadata.EventSource, metadata.EventType)
	if err != nil {
		return nil, fmt.Errorf("ebus: 获取事件工厂失败: %w", err)
	}

	event, err := factory()
	if err != nil {
		return nil, fmt.Errorf("ebus: 创建事件实例失败: %w", err)
	}

	if err := json.Unmarshal(envelope.Payload, event); err != nil {
		return nil, fmt.Errorf("ebus: 事件(%s)解码失败: %w", metadata.EventId, err)
	}

	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("ebus: 事件(%s)无效: %w", metadata.EventId, err)
	}

	eventMetadata := event.Metadata()
	if eventMetadata == nil {
		return nil, fmt.Errorf("ebus: 事件(%s)元数据为空", metadata.EventId)
	}

	if eventMetadata.SchemaVersion != metadata.SchemaVersion {
		return nil, fmt.Errorf("ebus: 事件(%s)元数据[模型版本]不匹配", metadata.EventId)
	}

	if eventMetadata.EventId != metadata.EventId {
		return nil, fmt.Errorf("ebus: 事件(%s)元数据[事件ID]不匹配", metadata.EventId)
	}

	if eventMetadata.EventSource != metadata.EventSource {
		return nil, fmt.Errorf("ebus: 事件(%s)元数据[事件来源]不匹配", metadata.EventId)
	}

	if eventMetadata.EventType != metadata.EventType {
		return nil, fmt.Errorf("ebus: 事件(%s)元数据[事件类型]不匹配", metadata.EventId)
	}

	// 忽略事件时间的检查

	return event, nil
}

// Subscribe 订阅事件
func (sub *subscriber) Subscribe(ctx context.Context, topic string, group string, handler EventHandler) (string, error) {
	topic = strings.TrimSpace(topic)
	if len(topic) == 0 {
		return "", fmt.Errorf("ebus: 订阅主题不能为空")
	}

	group = strings.TrimSpace(group)
	if len(group) == 0 {
		return "", fmt.Errorf("ebus: 订阅组不能为空")
	}

	if handler == nil {
		return "", fmt.Errorf("ebus: 事件处理函数不能为空")
	}

	wrapHandler := func(ctx context.Context, delivery *broker.Delivery) (finalErr error) {
		defer func() {
			if panicInfo := recover(); panicInfo != nil {
				finalErr = fmt.Errorf("ebus: 事件处理函数发生 panic: %v\n\n%s", panicInfo, debug.Stack())
			}
		}()

		if delivery == nil {
			return fmt.Errorf("ebus: 接收到空的投递")
		}

		msgTopic := strings.TrimSpace(delivery.Topic)
		if len(msgTopic) == 0 {
			return fmt.Errorf("ebus: 接收到空的主题")
		}

		if len(delivery.Message.Body) == 0 {
			return fmt.Errorf("ebus: 接收到空的消息体")
		}

		contentType := delivery.Message.ContentType
		contentType = strings.TrimSpace(contentType)
		contentType = strings.ToLower(contentType)
		if !strings.HasPrefix(contentType, ContentTypeJson) {
			return fmt.Errorf("ebus: 不支持的内容类型: %s", contentType)
		}

		event, err := sub.decodeEvent(delivery.Message.Body)
		if err != nil {
			return err
		}

		if err := handler(ctx, msgTopic, event); err != nil {
			return fmt.Errorf("ebus: 事件(%s)处理失败: %w", event.Metadata().EventId, err)
		}

		return nil
	}

	return sub.inner.Subscribe(ctx, topic, wrapHandler, broker.WithSubscribeGroup(group))
}

// Unsubscribe 取消订阅
func (sub *subscriber) Unsubscribe(ctx context.Context, subscriptionId string) error {
	return sub.inner.Unsubscribe(ctx, subscriptionId)
}

// Close 关闭订阅者
func (sub *subscriber) Close() error {
	return sub.inner.Close()
}
