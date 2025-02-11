package pbservice

import (
	"log"
	"strconv"

	"umich.edu/eecs491/proj2/viewservice"
)

// additions to PBServer state.
type PBServerImpl struct {
	currentView viewservice.View
	server      server
}

type server struct {
	putter        chan<- OpArgs
	appender      chan<- OpArgs
	getter        chan<- OpArgs
	replier       <-chan OpReply
	sendPusher    chan<- interface{}
	pushReplier   <-chan PushReply
	receivePusher chan<- PushArgs
	end           chan interface{}
}

func (s server) put(args OpArgs) OpReply {
	// log.Println("Put called")
	s.putter <- args
	// log.Println("Put done")
	return <-s.replier
}

func (s server) append(args OpArgs) OpReply {
	s.appender <- args
	return <-s.replier
}

func (s server) get(args OpArgs) OpReply {
	s.getter <- args
	return <-s.replier
}

func (s server) sendPush() PushReply {
	s.sendPusher <- struct{}{}
	return <-s.pushReplier
}

func (s server) receivePush(args PushArgs) {
	s.receivePusher <- args
}

func (pb *PBServer) makeServer() server {
	var result server

	putter := make(chan OpArgs)
	appender := make(chan OpArgs)
	getter := make(chan OpArgs)
	replier := make(chan OpReply)
	sendPusher := make(chan interface{})
	pushReplier := make(chan PushReply)
	receivePusher := make(chan PushArgs)

	end := make(chan interface{})
	result.putter = putter
	result.appender = appender
	result.getter = getter
	result.replier = replier
	result.sendPusher = sendPusher
	result.pushReplier = pushReplier
	result.receivePusher = receivePusher
	result.end = end

	go func() {
		database := make(map[string]string)
		opcache := make(map[string]Result)
		reply := &OpReply{Err: OK}
		for {
			select {
			case args := <-putter:
				database[args.Key] = args.Value
				reply = &OpReply{Err: OK}
				opcache[args.Client+":"+strconv.Itoa(args.SeqNo)] = Result{SeqNo: args.SeqNo, V: *reply}
				replier <- *reply

			case args := <-appender:
				reply = &OpReply{Err: OK}
				database[args.Key] += args.Value
				opcache[args.Client+":"+strconv.Itoa(args.SeqNo)] = Result{SeqNo: args.SeqNo, V: *reply}
				replier <- *reply

			case args := <-getter:
				log.Println("Get received: ", args)
				value := database[args.Key]
				log.Println("Get value: ", value)
				reply = &OpReply{Err: OK, Value: value}
				opcache[args.Client+":"+strconv.Itoa(args.SeqNo)] = Result{SeqNo: args.SeqNo, V: *reply}
				replier <- *reply
			case <-sendPusher:
				var pushReply PushReply
				log.Println("sendPush received")
				log.Printf("Push: primary=%s, backup=%s\n", pb.me, pb.impl.currentView.Backup)
				ok := call(pb.impl.currentView.Backup, "PBServer.Push", PushArgs{KVStore: database, OpCache: opcache, View: pb.impl.currentView}, &pushReply)
				if !ok {
					log.Println("Push failed error ", pushReply.Err)
				}
				pushReplier <- pushReply
			case args := <-receivePusher:
				log.Println("Pulling")
				database = args.KVStore
				log.Println(database["1"])
				opcache = args.OpCache
				pb.impl.currentView = args.View
			}

		}

	}()
	return result
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	pb.impl.server = pb.makeServer()
}

func (pb *PBServer) NeedForward() bool {
	return pb.impl.currentView.Primary == pb.me && pb.impl.currentView.Backup != ""
}

// On the backup side, if you receive an operation from
func (pb *PBServer) WrongServer(args OpArgs) bool {
	return (pb.impl.currentView.Primary != pb.me && args.Source != pb.impl.currentView.Primary) ||
		(args.Source != args.Client && args.Source != pb.impl.currentView.Primary)
}

// server Operation() RPC handler.
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
	// log.Println("Operation called by ", args.Source, " for ", args.Op, args.Key, args.Value, "current Primary is ", pb.impl.currentView.Primary, "current Backup is ", pb.impl.currentView.Backup)
	//log.Println("I'm ", pb.me)
	// log.Println("Operation received from ", args.Source)
	forwardArgs := args
	forwardArgs.Source = pb.me
	var forwardReply OpReply
	// First forward to backup if primary
	if pb.NeedForward() {
		log.Println("Forwarding to backup")
		log.Println("Backup is ", pb.impl.currentView.Backup)
		ok := call(pb.impl.currentView.Backup, "PBServer.Operation", forwardArgs, &forwardReply)
		if !ok {
			// log.Println("Forwarding to backup failed")
			reply.Err = ErrWrongServer
			return nil
		}
	}
	// Check if error from forwarding with backup
	if forwardReply.Err == ErrWrongServer || pb.WrongServer(args) {
		// log.Println("Wrong server")
		reply.Err = ErrWrongServer
		return nil
	}

	switch args.Op {
	case "Put":
		*reply = pb.impl.server.put(args)
	case "Append":
		*reply = pb.impl.server.append(args)
	case "Get":
		*reply = pb.impl.server.get(args)
	}

	// log.Println(reply)
	return nil
}

// server Push() RPC handler
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	log.Println("Push received")
	pb.impl.server.receivePush(args)
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) NeedPush(latestView viewservice.View) bool {
	return pb.impl.currentView.Primary == pb.me && latestView.Backup != pb.impl.currentView.Backup
}

// func (pb *PBServer) BackupPromoted(latestView viewservice.View) bool {
// 	return pb.impl.currentView.Backup == pb.me && latestView.Primary != pb.impl.currentView.Primary
// }

func (pb *PBServer) tick() {
	latestView, err := pb.vs.Ping(pb.impl.currentView.Viewnum)
	if err != nil {
		return
	}
	if pb.NeedPush(latestView) {
		pb.impl.currentView = latestView
		pb.impl.server.sendPush()
	} else {
		pb.impl.currentView = latestView
	}

}
