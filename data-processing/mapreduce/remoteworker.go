package mapreduce

import (
	"io"
	"net/rpc"
	"time"
)

type RemoteWorker struct {
	id       int
	hostname string
	status   string
}

func (worker *RemoteWorker) callRemoteWorker(proc string, args any, reply any) error {
	var (
		err    error
		client *rpc.Client
	)
	client, err = rpc.Dial("tcp", worker.hostname)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Call(proc, args, reply)
	if err == io.ErrUnexpectedEOF {
		var tmpClient *rpc.Client
		time.Sleep(time.Second)
		tmpClient, err = rpc.Dial("tcp", worker.hostname)
		if err == nil {
			tmpClient.Close()
			return nil
		}
		return err
	} else if err != nil {
		return err
	}
	return nil
}
