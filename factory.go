package ebus

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	ErrEventFactoryExists   = errors.New("ebus: 事件工厂已存在")
	ErrEventFactoryNotFound = errors.New("ebus: 事件工厂不存在")
)

// EventFactory 事件工厂函数
//
// 用于解码时创建事件实例
type EventFactory func() (Event, error)

var (
	eventFactoryRegistry     = map[string]EventFactory{} // 事件工厂注册表
	eventFactoryRegistryLock = sync.RWMutex{}            // 事件工厂注册表锁
)

func buildEventFactoryKey(scmVersion SchemaVersion, evtSource EventSource, evtType EventType) string {
	return string(scmVersion) + "|" + string(evtSource) + "|" + string(evtType)
}

// RegisterEventFactory 注册事件工厂
// - scmVersion 模型版本
// - evtSource  事件来源
// - evtType    事件类型
// - evtFactory 事件工厂
func RegisterEventFactory(scmVersion SchemaVersion, evtSource EventSource, evtType EventType, evtFactory EventFactory) error {
	scmVersion = scmVersion.Normalize()
	if scmVersion.IsEmpty() {
		return fmt.Errorf("ebus: 模型版本不能为空")
	}

	evtSource = evtSource.Normalize()
	if evtSource.IsEmpty() {
		return fmt.Errorf("ebus: 事件来源不能为空")
	}

	evtType = evtType.Normalize()
	if evtType.IsEmpty() {
		return fmt.Errorf("ebus: 事件类型不能为空")
	}

	if evtFactory == nil {
		return fmt.Errorf("ebus: 事件工厂不能为空")
	}

	// 在锁外面构建key, 减少锁的持有时间
	factoryKey := buildEventFactoryKey(scmVersion, evtSource, evtType)

	eventFactoryRegistryLock.Lock()
	defer eventFactoryRegistryLock.Unlock()

	if _, exists := eventFactoryRegistry[factoryKey]; exists {
		return fmt.Errorf("%w: %s", ErrEventFactoryExists, factoryKey)
	} else {
		eventFactoryRegistry[factoryKey] = evtFactory
	}

	return nil
}

// MustRegisterEventFactory 注册事件工厂, 如果注册失败则 panic
// - scmVersion 模型版本
// - evtSource  事件来源
// - evtType    事件类型
// - evtFactory 事件工厂
func MustRegisterEventFactory(scmVersion SchemaVersion, evtSource EventSource, evtType EventType, evtFactory EventFactory) {
	if err := RegisterEventFactory(scmVersion, evtSource, evtType, evtFactory); err != nil {
		panic(err)
	}
}

// GetEventFactory 获取事件工厂
// - scmVersion 模型版本
// - evtSource  事件来源
// - evtType    事件类型
func GetEventFactory(scmVersion SchemaVersion, evtSource EventSource, evtType EventType) (EventFactory, error) {
	scmVersion = scmVersion.Normalize()
	if scmVersion.IsEmpty() {
		return nil, fmt.Errorf("ebus: 模型版本不能为空")
	}

	evtSource = evtSource.Normalize()
	if evtSource.IsEmpty() {
		return nil, fmt.Errorf("ebus: 事件来源不能为空")
	}

	evtType = evtType.Normalize()
	if evtType.IsEmpty() {
		return nil, fmt.Errorf("ebus: 事件类型不能为空")
	}

	// 在锁外面构建key, 减少锁的持有时间
	factoryKey := buildEventFactoryKey(scmVersion, evtSource, evtType)

	eventFactoryRegistryLock.RLock()
	defer eventFactoryRegistryLock.RUnlock()

	if factory, exists := eventFactoryRegistry[factoryKey]; exists {
		return factory, nil
	} else {
		return nil, fmt.Errorf("%w: %s", ErrEventFactoryNotFound, factoryKey)
	}
}

// ExistsEventFactory 检查事件工厂是否存在
// - scmVersion 模型版本
// - evtSource  事件来源
// - evtType    事件类型
func ExistsEventFactory(scmVersion SchemaVersion, evtSource EventSource, evtType EventType) bool {
	scmVersion = scmVersion.Normalize()
	if scmVersion.IsEmpty() {
		return false
	}

	evtSource = evtSource.Normalize()
	if evtSource.IsEmpty() {
		return false
	}

	evtType = evtType.Normalize()
	if evtType.IsEmpty() {
		return false
	}

	// 在锁外面构建key, 减少锁的持有时间
	factoryKey := buildEventFactoryKey(scmVersion, evtSource, evtType)

	eventFactoryRegistryLock.RLock()
	defer eventFactoryRegistryLock.RUnlock()

	_, exists := eventFactoryRegistry[factoryKey]
	return exists
}

// ListEventFactoryKeys 列出已注册的事件工厂键
//
// 返回的键格式为 "模型版本|事件来源|事件类型"
func ListEventFactoryKeys() []string {
	eventFactoryRegistryLock.RLock()
	defer eventFactoryRegistryLock.RUnlock()

	keys := make([]string, 0, len(eventFactoryRegistry))
	for key := range eventFactoryRegistry {
		keys = append(keys, key)
	}

	slices.Sort(keys)
	return keys
}
