package pbservice

import (
	"log"

	"umich.edu/eecs491/proj2/viewservice"
)

// additions to PBServer state.
type PBServerImpl struct {
	currentView viewservice.View
	// channels for the goroutine
	operator      chan OpArgs
	replier       chan OpReply
	ticker        chan interface{}
	sendPusher    chan interface{}
	pushReplier   chan PushReply
	receivePusher chan PushArgs
	end           chan interface{}
}

func (impl *PBServerImpl) doOperation(args OpArgs) OpReply {
	impl.operator <- args
	return <-impl.replier
}

func (impl *PBServerImpl) doTick() {
	impl.ticker <- struct{}{}
}

func (impl *PBServerImpl) sendPush() PushReply {
	impl.sendPusher <- struct{}{}
	return <-impl.pushReplier
}

func (impl *PBServerImpl) receivePush(args PushArgs) {
	impl.receivePusher <- args
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	// Create channels
	pb.impl.operator = make(chan OpArgs)
	pb.impl.ticker = make(chan interface{})
	pb.impl.replier = make(chan OpReply)
	pb.impl.sendPusher = make(chan interface{})
	pb.impl.pushReplier = make(chan PushReply)
	pb.impl.receivePusher = make(chan PushArgs)
	pb.impl.end = make(chan interface{})
	pb.impl.currentView = viewservice.View{}

	// Start the goroutine
	go func() {
		database := make(map[string]string)
		opcache := make(map[string]Result)

		for {
			select {
			case args := <-pb.impl.operator:

				if _, exists := opcache[args.Client]; exists && args.SeqNo <= opcache[args.Client].SeqNo {
					log.Printf("Duplicate operation from %s\n", args.Client)
					pb.impl.replier <- opcache[args.Client].V
					continue
				}
				// This is the new, single point for all client operations.
				reply := OpReply{Err: OK} // We'll fill this in.

				// 1) Print debug logs (moved from Operation())
				log.Printf("Operation called by %s for %s key=%s val=%s; current P=%s B=%s\n",
					args.Source, args.Op, args.Key, args.Value,
					pb.impl.currentView.Primary, pb.impl.currentView.Backup)
				log.Println("I'm", pb.me)
				log.Println("Current view: ", pb.impl.currentView)

				// 2) Possibly forward to backup
				var forwardReply OpReply
				if pb.NeedForward() {
					log.Println("Forwarding to backup:", pb.impl.currentView.Backup)
					fargs := args
					fargs.Source = pb.me

					ok := call(pb.impl.currentView.Backup, "PBServer.Operation", fargs, &forwardReply)

					// Requeue in case of failure
					if !ok {
						break
					}

					// 3) Check if error from forwarding
					if forwardReply.Err == ErrWrongServer || pb.WrongServerOp(args) {
						reply.Err = ErrWrongServer
						pb.impl.replier <- reply
						continue
					}

				}

				// 4) Actually apply the operation locally
				switch args.Op {
				case "Put":
					database[args.Key] = args.Value
				case "Append":
					database[args.Key] += args.Value
				case "Get":
					val := database[args.Key]
					reply.Value = val
				default:
					reply.Err = "UnknownOp"
				}

				// If you want to store results in opcache, do so:
				opcache[args.Client] = Result{SeqNo: args.SeqNo, V: reply}

				// 5) Return final reply
				pb.impl.replier <- reply
			case <-pb.impl.ticker:
				log.Println("Received tick")
				latestView, err := pb.vs.Ping(pb.impl.currentView.Viewnum)
				if err != nil {
					log.Println("Ping error", err)
					return
				}
				if pb.NeedPush(latestView) {
					pb.impl.currentView = latestView
					pb.impl.sendPush()
				} else {
					pb.impl.currentView = latestView
				}
				log.Println("Current view: ", pb.impl.currentView)
			case <-pb.impl.sendPusher:
				// Pushing logic remains the same
				pargs := PushArgs{
					KVStore: database,
					OpCache: opcache,
					View:    pb.impl.currentView,
				}
				var pushReply PushReply
				log.Println("sendPush received")
				log.Printf("Push: primary=%s, backup=%s\n",
					pb.me, pb.impl.currentView.Backup)
				log.Println(database["1"])

				for {
					ok := call(pb.impl.currentView.Backup, "PBServer.Push", pargs, &pushReply)
					// Push successful, break out of loop
					if ok {
						break
					}
					if !ok {
						log.Println("Push failed error", pushReply.Err)
					}
					// Able to make connection with backup, but backup identifies split brain
					if pushReply.Err == ErrWrongServer {
						pushReply.Err = ErrWrongServer
						pb.impl.pushReplier <- pushReply
						continue
					}
				}

			case pushArgs := <-pb.impl.receivePusher:
				log.Println("Pulling on backup")
				database = pushArgs.KVStore
				opcache = pushArgs.OpCache
				pb.impl.currentView = pushArgs.View
				log.Printf(database["1"])

			}
		}
	}()
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
	*reply = pb.impl.doOperation(args)
	return nil
}

// server Push() RPC handler
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	log.Println("Push received")
	pb.impl.receivePush(args)
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
	pb.impl.doTick()
}
