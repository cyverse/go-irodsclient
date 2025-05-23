package common

type TransferTrackerCallback func(processed int64, total int64)
type ConnectionTrackerCallback func(established int, terminated int)
