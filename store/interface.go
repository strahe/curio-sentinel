package store

import "context"

// Store 定义状态存储接口
type Store interface {
    // Get 获取数据
    Get(ctx context.Context, key string) ([]byte, error)

    // Set 设置数据
    Set(ctx context.Context, key string, value []byte) error

    // Delete 删除数据
    Delete(ctx context.Context, key string) error

    // Close 关闭存储
    Close() error
}
