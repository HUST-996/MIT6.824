package raft

// logging/util.go

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

// 初始化日志系统，读取环境变量控制日志详细程度
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	// 禁用默认的日期和时间
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// 从环境变量获取日志详细程度
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("无效的详细程度 %v", v)
		}
	}
	return level
}

// 打印调试信息
func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 { // 如果日志详细度大于等于 1，则打印日志
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
