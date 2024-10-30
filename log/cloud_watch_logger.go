package log

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/mcontext"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type CloudWatchLogger struct {
	client         *cloudwatchlogs.Client
	level          zapcore.Level
	logGroup       string
	logStream      string
	nextToken      *string
	additionalData map[string]interface{}
	name           string
	callDepth      int
	isSimplify     bool
	logBuffer      []types.InputLogEvent
	mu             sync.Mutex
	batchSize      int
	flushTicker    *time.Ticker
}

// 创建一个 CloudWatchLogger 实例
func NewCloudWatchLogger(accessKey string, secretKey string, region string, logGroup, logStream string, logLevel int) (*CloudWatchLogger, error) {
	credential := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
		accessKey,
		secretKey,
		"",
	))
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credential),
	)
	// Create CloudWatch Logs client
	client := cloudwatchlogs.NewFromConfig(cfg)

	// Ensure log group and log stream exist
	err = ensureLogGroupAndStream2(client, logGroup, logStream)
	if err != nil {
		return nil, err
	}
	logger := &CloudWatchLogger{
		client:         client,
		logGroup:       logGroup,
		level:          logLevelMap[logLevel],
		logStream:      logStream,
		additionalData: make(map[string]interface{}),
		logBuffer:      []types.InputLogEvent{},
		batchSize:      200,
		flushTicker:    time.NewTicker(2 * time.Second),
	}
	go logger.startFlushRoutine()
	return logger, nil
}

func (cw *CloudWatchLogger) startFlushRoutine() {
	for {
		<-cw.flushTicker.C
		cw.lockFlushLogs() // 在每个定时器周期内尝试发送日志
	}
}

func (cw *CloudWatchLogger) lockFlushLogs() {
	cw.mu.Lock()
	cw.flushLogs()
	cw.mu.Unlock()
}

func (cw *CloudWatchLogger) flushLogs() {
	if len(cw.logBuffer) == 0 {
		return // 如果没有日志，直接返回
	}
	limit := cw.batchSize
	if len(cw.logBuffer) < cw.batchSize {
		limit = len(cw.logBuffer)
	}
	batch := cw.logBuffer[:limit]

	go cw.BatchPutLogEvents(batch)

	cw.logBuffer = cw.logBuffer[limit:]
}

func (cw *CloudWatchLogger) BatchPutLogEvents(logBuffer []types.InputLogEvent) {
	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(cw.logGroup),
		LogStreamName: aws.String(cw.logStream),
		LogEvents:     logBuffer,
	}

	// If we have a nextToken, use it
	if cw.nextToken != nil {
		input.SequenceToken = cw.nextToken
	}
	// Send the log events to CloudWatch Logs
	output, err := cw.client.PutLogEvents(context.TODO(), input)
	if err != nil {
		fmt.Println("上传日志到 CloudWatch 失败:", err)
	} else {
		cw.nextToken = output.NextSequenceToken
	}
}

func InitCloudWatchLoggerConfig(
	moduleName string,
	logLevel int,
	accessKey string, secretKey string, region string,
	logGroup string,
) error {

	l, err := NewCloudWatchLogger(accessKey, secretKey, region, logGroup, moduleName, logLevel)
	if err != nil {
		return err
	}

	pkgLogger = l
	return nil
}

func (cw *CloudWatchLogger) Debug(ctx context.Context, msg string, keysAndValues ...any) {
	if cw.level > zapcore.DebugLevel {
		return
	}
	cw.log(ctx, "DEBUG", msg, nil, keysAndValues...)
}

func (cw *CloudWatchLogger) Info(ctx context.Context, msg string, keysAndValues ...any) {
	if cw.level > zapcore.InfoLevel {
		return
	}
	cw.log(ctx, "INFO", msg, nil, keysAndValues...)
}

func (cw *CloudWatchLogger) Warn(ctx context.Context, msg string, err error, keysAndValues ...any) {
	if cw.level > zapcore.WarnLevel {
		return
	}
	cw.log(ctx, "WARN", msg, err, keysAndValues...)
}

// 实现 Error 方法
func (cw *CloudWatchLogger) Error(ctx context.Context, msg string, err error, keysAndValues ...any) {
	if cw.level > zapcore.ErrorLevel {
		return
	}
	cw.log(ctx, "ERROR", msg, err, keysAndValues...)
}

// WithValues 实现
func (cw *CloudWatchLogger) WithValues(keysAndValues ...any) Logger {
	newLogger := *cw
	for i := 0; i < len(keysAndValues); i += 2 {
		key := fmt.Sprintf("%v", keysAndValues[i])
		value := keysAndValues[i+1]
		newLogger.additionalData[key] = value
	}
	return &newLogger
}

// WithName 实现
func (cw *CloudWatchLogger) WithName(name string) Logger {
	newLogger := *cw
	newLogger.name = name
	return &newLogger
}

// WithCallDepth 实现
func (cw *CloudWatchLogger) WithCallDepth(depth int) Logger {
	newLogger := *cw
	newLogger.callDepth = depth
	return &newLogger
}

func (cw *CloudWatchLogger) kvAppend(ctx context.Context, keysAndValues []any) []any {
	if ctx == nil {
		return keysAndValues
	}
	operationID := mcontext.GetOperationID(ctx)
	opUserID := mcontext.GetOpUserID(ctx)
	connID := mcontext.GetConnID(ctx)
	triggerID := mcontext.GetTriggerID(ctx)
	opUserPlatform := mcontext.GetOpUserPlatform(ctx)
	remoteAddr := mcontext.GetRemoteAddr(ctx)

	if cw.isSimplify {
		if len(keysAndValues)%2 == 0 {
			for i := 1; i < len(keysAndValues); i += 2 {

				if val, ok := keysAndValues[i].(LogFormatter); ok && val != nil {
					keysAndValues[i] = val.Format()
				}
			}
		} else {
			ZError(ctx, "keysAndValues length is not even", nil)
		}
	}

	if opUserID != "" {
		keysAndValues = append([]any{constant.OpUserID, opUserID}, keysAndValues...)
	}
	if operationID != "" {
		keysAndValues = append([]any{constant.OperationID, operationID}, keysAndValues...)
	}
	if connID != "" {
		keysAndValues = append([]any{constant.ConnID, connID}, keysAndValues...)
	}
	if triggerID != "" {
		keysAndValues = append([]any{constant.TriggerID, triggerID}, keysAndValues...)
	}
	if opUserPlatform != "" {
		keysAndValues = append([]any{constant.OpUserPlatform, opUserPlatform}, keysAndValues...)
	}
	if remoteAddr != "" {
		keysAndValues = append([]any{constant.RemoteAddr, remoteAddr}, keysAndValues...)
	}
	return keysAndValues
}

// 日志发送方法
func (cw *CloudWatchLogger) log(ctx context.Context, level, msg string, err error, keysAndValues ...any) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in CloudWatchLogger: %v\n", r)
		}
	}()

	keysAndValues = cw.kvAppend(ctx, keysAndValues)

	allKeysAndValues := make(map[string]interface{})

	// 添加 CloudWatchLogger 中的全局附加数据
	for k, v := range cw.additionalData {
		allKeysAndValues[k] = v
	}

	if len(keysAndValues)%2 == 0 { // 检查是否为成对的键值对
		for i := 0; i < len(keysAndValues); i += 2 {
			key, ok := keysAndValues[i].(string)
			if !ok {
				key = fmt.Sprintf("invalid_key_%d", i/2)
			}
			value := formatValue(keysAndValues[i+1])
			allKeysAndValues[key] = value
		}
	} else {
		fmt.Println("Warning: keysAndValues 数量不成对，忽略未配对的元素")
	}

	if err != nil {
		allKeysAndValues["error"] = err.Error()
	}

	// 格式化日志信息
	kvString := ""
	for k, v := range allKeysAndValues {
		kvString += fmt.Sprintf("%s=%v ", k, v)
	}

	logMessage := fmt.Sprintf("[%s] %s - %s", level, msg, kvString)

	logEvent := types.InputLogEvent{
		Message:   aws.String(logMessage),
		Timestamp: aws.Int64(time.Now().Unix() * 1000),
	}

	cw.mu.Lock()
	cw.logBuffer = append(cw.logBuffer, logEvent)
	if len(cw.logBuffer) > cw.batchSize {
		fmt.Println("上传日志到 CloudWatch size:", len(cw.logBuffer))
		cw.flushLogs()
	}
	cw.mu.Unlock()
}

func formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer: // 支持实现了 String 方法的类型
		return v.String()
	case error:
		return v.Error()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// 确保日志组和日志流已创建
func ensureLogGroupAndStream2(client *cloudwatchlogs.Client, logGroup, logStream string) error {
	_, err := client.CreateLogGroup(context.TODO(), &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(logGroup),
	})
	if err != nil && !strings.Contains(err.Error(), "ResourceAlreadyExistsException") {
		return err
	}

	_, err = client.CreateLogStream(context.TODO(), &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
	})
	if err != nil && !strings.Contains(err.Error(), "ResourceAlreadyExistsException") {
		return err
	}

	return nil
}
