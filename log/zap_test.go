package log

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 异步方法
func asyncMethod() {
	time.Sleep(2 * time.Second) // 模拟耗时操作
	fmt.Println("异步方法执行完成")
}

// 主函数
func TestSDKLog2(t *testing.T) {
	// 启动一个 goroutine 以异步执行 asyncMethod
	go asyncMethod() // 使用 go 关键字启动异步执行

	// 在主函数中继续执行其他操作
	fmt.Println("主函数继续执行...")

	// 等待一段时间，确保异步方法完成
	time.Sleep(3 * time.Second)
	fmt.Println("主函数执行完成")
}

// TestSDKLog tests the SDKLog function for proper log output including custom [file:line] information
func TestSDKLog(t *testing.T) {

	l, err := NewCloudWatchLogger("", "", "",
		"", "testLogger", 6)
	assert.NoError(t, err)

	for i := 0; i < 1000; i++ {
		if i%100 == 0 {
			time.Sleep(1 * time.Second)
		}
		l.Debug(context.Background(), "fffffffxxx  第1030次 ", "key", i)
		l.Info(context.TODO(), "msg11111 第1030次", "key", i)
		l.Debug(context.TODO(), "msg 第1030次")
	}

	select {}
	// assert.Contains(t, output, "This is a test message")
	// assert.Contains(t, output, "[TestSDK/TestPlatform]")
	// assert.Contains(t, output, "[test_file.go:123]")
	// assert.Contains(t, output, "key")
	// assert.Contains(t, output, "value")
}
