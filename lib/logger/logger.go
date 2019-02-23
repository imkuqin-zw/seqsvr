package logger

import (
	"go.uber.org/zap"
	"fmt"
	"go.uber.org/zap/zapcore"
	"time"
)

var Logger *zap.Logger

func InitLogger(cfg *zap.Config) {
	var err error
	if cfg.Development {
		cfg.EncoderConfig = zap.NewDevelopmentEncoderConfig()
	} else {
		cfg.EncoderConfig = zap.NewProductionEncoderConfig()
	}
	if cfg.Encoding == "console" {
		cfg.EncoderConfig.EncodeTime = timeEncoder
	} else {
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}
	Logger, err = cfg.Build()
	if err != nil {
		panic(err)
	}
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("[2006-01-02 15:04:05]"))
}

func Info(msg string, field ...zap.Field) {
	Logger.Info(msg, field...)
}

func Infof(msg string, field ...interface{}) {
	Logger.Info(fmt.Sprintf(msg, field...))
}

func Debug(msg string, field ...zap.Field) {
	Logger.Debug(msg, field...)
}

func Error(msg string, field ...zap.Field) {
	Logger.Error(msg, field...)
}

func Warn(msg string, field ...zap.Field) {
	Logger.Warn(msg, field...)
}

func DPanic(msg string, field ...zap.Field) {
	Logger.DPanic(msg, field...)
}

func Panic(msg string, field ...zap.Field) {
	Logger.Panic(msg, field...)
}

func Panicf(msg string, field ...interface{}) {
	Logger.Panic(fmt.Sprintf(msg, field...))
}

func Fatal(msg string, field ...zap.Field) {
	Logger.Fatal(msg, field...)
}

func Fatalf(msg string, field ...interface{}) {
	Logger.Fatal(fmt.Sprintf(msg, field...))
}

func Sync() {
	Logger.Sync()
}
