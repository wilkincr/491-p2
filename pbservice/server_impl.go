package pbservice

import (
	"log"
	"time"

	"umich.edu/eecs491/proj2/viewservice"
)

// additions to PBServer state.
type PBServerImpl struct {
	currentView viewservice.View
	// channels for the goroutine
	operator chan Operation
	ticker   chan struct{}
	pusher   chan Push
	end      chan interface{}
}

type Operation struct {
	args    OpArgs
	replyCh chan OpReply
}

type Push struct {
	args    PushArgs
	replyCh chan PushReply
}

func (impl *PBServerImpl) forwardOperation(args OpArgs, primary string, backup string, forwardReply *OpReply) bool {
	// log.Println("Forwarding to backup:", backup)
	fargs := args
	fargs.Source = primary
	// Force retry until success

	ok := call(backup, "PBServer.Operation", fargs, &forwardReply)
	// log.Println("Forward reply: ", forwardReply)
	return ok
}

func (pb *PBServer) runServer() {
	database := make(map[string]string)
	opcache := make(map[string]Result)
	pb.impl.currentView = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}

	UpdateView := func() {
		latestView, err := pb.vs.Ping(pb.impl.currentView.Viewnum)
		if latestView.Viewnum != pb.impl.currentView.Viewnum {
			// log.Println("Old view: ", pb.impl.currentView)
			// log.Println("New view: ", latestView)
		}
		if err != nil {
			//log.Println("Ping error", err)
			return
		}
		if pb.NeedPush(latestView) {
			pushRetryNeeded := true
			pb.impl.currentView = latestView

			for pushRetryNeeded {
				pargs := PushArgs{View: latestView, KVStore: database, OpCache: opcache}
				var pushReply PushReply
				log.Println("Pushing database")
				for k, v := range database {
					log.Println(k, v)
				}
				ok := call(pb.impl.currentView.Backup, "PBServer.Push", pargs, &pushReply)
				if ok {
					pushRetryNeeded = false
					break
				}
				if !ok {
					log.Println("Push failed")
				}
				latestView, err := pb.vs.Ping(pb.impl.currentView.Viewnum)
				if err != nil {
					pb.impl.currentView = latestView
				}
				if pb.impl.currentView.Backup == "" {
					pushRetryNeeded = false
				}
				time.Sleep(viewservice.PingInterval)
			}

		} else {
			pb.impl.currentView = latestView
		}
		// log.Println("Current view: ", pb.impl.currentView)
	}

	for {
		// log.Println("Waiting for operation on", pb.me)
		select {
		case operation := <-pb.impl.operator:
			// log.Println("Operation received on", pb.me, "current database:")
			// for k, v := range database {
			// 	log.Println(k, v)
			// }
			args := operation.args
			// log.Println("Operation called: ", args.Source, args.Op, args.Key, args.Value, args.Client, args.SeqNo)
			// log.Println("My view: ", pb.impl.currentView, pb.me)

			if _, exists := opcache[args.Client]; exists && args.SeqNo <= opcache[args.Client].SeqNo {
				// log.Println("Duplicate operation from client", args.Client, args.SeqNo)
				operation.replyCh <- opcache[args.Client].V
				continue
			}
			// This is the new, single point for all client operations.
			var operationReply OpReply
			operationReply.Err = OK
			// 1) Print debug logs (moved from Operation())
			// log.Printf("Operation called by %s for %s key=%s val=%s; current P=%s B=%s\n",
			// 	args.Source, args.Op, args.Key, args.Value,
			// 	pb.impl.currentView.Primary, pb.impl.currentView.Backup)
			//log.Println("I'm", pb.me)
			if pb.WrongServerOp(args) {
				log.Println("WrongServerOp", args.Source, args.Op, args.Key, args.Value, args.Client, args.SeqNo)
				operationReply.Err = ErrWrongServer
				operation.replyCh <- operationReply
				continue
			}

			// 2) Possibly forward to backup
			var forwardReply OpReply

			if pb.NeedForward() {
				// log.Println("About to forward, I'm", pb.me)
				fargs := args
				fargs.Source = pb.me
				retryNeeded := true
				for retryNeeded {
					ok := call(pb.impl.currentView.Backup, "PBServer.Operation", fargs, &forwardReply)
					// log.Println("Forward reply: ", forwardReply)
					if ok {
						retryNeeded = false
						break
					} else if !ok {
						log.Println("Forward failed, retrying")
					}

					// Refresh view
					UpdateView()
					time.Sleep(viewservice.PingInterval)
				}
			}

			// 3) Check if error from forwarding
			if forwardReply.Err == ErrWrongServer {
				log.Println("WrongServerOp", args.Source, args.Op, args.Key, args.Value, args.Client, args.SeqNo)
				operationReply.Err = ErrWrongServer
				operation.replyCh <- operationReply
				continue
			}

			// Apply the operation locally
			switch args.Op {
			case "Put":
				database[args.Key] = args.Value
			case "Append":
				database[args.Key] += args.Value
			case "Get":
				val := database[args.Key]
				log.Println("Get value", val)
				operationReply.Value = val
			default:
				operationReply.Err = "UnknownOp"
			}

			opcache[args.Client] = Result{SeqNo: args.SeqNo, V: operationReply}
			operation.replyCh <- operationReply

		case <-pb.impl.ticker:
			// log.Println("Received tick")
			UpdateView()

		case push := <-pb.impl.pusher:
			var pushReply PushReply
			log.Println("Pulling on backup")
			database = push.args.KVStore
			opcache = push.args.OpCache
			log.Println("Printing database:")
			for k, v := range database {
				log.Println("ABC")
				log.Println(k, v)
			}
			pb.impl.currentView = push.args.View
			pushReply.Err = OK
			push.replyCh <- pushReply
		}
	}
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	// Create channels
	pb.impl.operator = make(chan Operation)
	pb.impl.ticker = make(chan struct{})
	pb.impl.pusher = make(chan Push)
	pb.impl.end = make(chan interface{})

	// Start the goroutine
	go pb.runServer()
}

func (pb *PBServer) NeedForward() bool {
	return pb.impl.currentView.Primary == pb.me && pb.impl.currentView.Backup != ""
}

// On the backup side, if you receive an operation from
func (pb *PBServer) WrongServerOp(args OpArgs) bool {
	return (pb.impl.currentView.Primary != pb.me && args.Source != pb.impl.currentView.Primary) ||
		(args.Source != args.Client && args.Source != pb.impl.currentView.Primary)
}

// func (pb *PBServer) WrongServerPush(args PushArgs) bool {
// 	return (pb.impl.currentView.Primary && args.Source != pb.impl.currentView.Primary) ||
// }

// server Operation() RPC handler.
func (pb *PBServer) Operation(args OpArgs, reply *OpReply) error {
	op := Operation{
		args:    args,
		replyCh: make(chan OpReply),
	}
	pb.impl.operator <- op
	*reply = <-op.replyCh
	return nil
}

// server Push() RPC handler
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	push := Push{
		args:    args,
		replyCh: make(chan PushReply),
	}
	pb.impl.pusher <- push
	*reply = <-push.replyCh
	return nil
}

// ping the viewserver periodically.
// if view changed:
//
//	transition to new view.
//	manage transfer of state from primary to new backup.
func (pb *PBServer) NeedPush(latestView viewservice.View) bool {
	// Case A: New backup
	// Case B: Same backup, but backup has restarted and lost state
	// Case C: Backup promoted to primary and new backup
	return (pb.impl.currentView.Primary == pb.me && latestView.Backup != pb.impl.currentView.Backup) ||
		(pb.impl.currentView.Primary == pb.me && latestView.Backup == pb.impl.currentView.Backup && pb.impl.currentView.Viewnum != latestView.Viewnum) ||
		(pb.impl.currentView.Backup == pb.me && latestView.Primary == pb.me && latestView.Backup != "")
}

// func (pb *PBServer) BackupPromoted(latestView viewservice.View) bool {
// 	return pb.impl.currentView.Backup == pb.me && latestView.Primary != pb.impl.currentView.Primary
// }

func (pb *PBServer) tick() {
	pb.impl.ticker <- struct{}{}
}
