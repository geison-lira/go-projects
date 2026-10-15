package mapreduce

type KeyValue struct {
	Key   string
	Value string
}

type Task struct {
	Map                MapFunc
	Shuffle            ShuffleFunc
	Reduce             ReduceFunc
	NumReduceJobs      int
	NumMapFiles        int
	InputChan          chan []byte
	OutputChan         chan []KeyValue
	InputFilePathChan  chan string
	OutputFilePathChan chan string
}

type (
	MapFunc     func([]byte) []KeyValue
	ReduceFunc  func([]KeyValue) []KeyValue
	ShuffleFunc func(*Task, string) int
)
