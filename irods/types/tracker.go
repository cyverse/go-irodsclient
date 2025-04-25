package types

type TrackerTaskInfo struct {
	TaskID          int
	SubTaskID       int
	TasksTotal      int
	StartOffset     int64
	Length          int64
	ProcessedLength int64
	Terminated      bool
}

type TrackerFileInfo struct {
	FileLength int64
	FileName   string
}

type TrackerCallBack func(taskInfo *TrackerTaskInfo, fileInfo *TrackerFileInfo)
