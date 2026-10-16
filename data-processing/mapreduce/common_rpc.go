package mapreduce

type RegisterArgs struct {
	WorkerHostname string
}

type RegisterReply struct {
	WorkerId   int
	ReduceJobs int
}

type RunArgs struct {
	Id       int
	FilePath string
}
