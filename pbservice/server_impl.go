package pbservice

import (
	"log"
	"strconv"

	"umich.edu/eecs491/proj2/viewservice"
)

// additions to PBServer state.
type PBServerImpl struct {
	currentView viewservice.View
	database    map[string]string
	opcache     map[string]Result
}

// your pb.impl.* initializations here.
func (pb *PBServer) initImpl() {
	pb.impl.database = make(map[string]string)
	pb.impl.opcache = make(map[string]Result)
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
	log.Println("Operation called by ", args.Source, " for ", args.Op, args.Key, args.Value, "current Primary is ", pb.impl.currentView.Primary, "current Backup is ", pb.impl.currentView.Backup)
	log.Println("I'm ", pb.me)
	// log.Println("Operation received from ", args.Source)
	forwardArgs := args
	forwardArgs.Source = pb.me
	var forwardReply OpReply
	log.Println(reply)
	// First forward to backup if primary
	if pb.NeedForward() {
		ok := call(pb.impl.currentView.Backup, "PBServer.Operation", forwardArgs, &forwardReply)
		if !ok {
			log.Println("Forwarding to backup failed")
			reply.Err = ErrWrongServer
			return nil
		}
	}
	// Check if error from forwarding with backup
	if forwardReply.Err == ErrWrongServer || pb.WrongServer(args) {
		log.Println("Wrong server")
		reply.Err = ErrWrongServer
		return nil
	}

	Op := args.Op
	if Op == "Put" {
		pb.impl.database[args.Key] = args.Value
	} else if Op == "Append" {
		pb.impl.database[args.Key] += args.Value
	} else if Op == "Get" {
		reply.Value = pb.impl.database[args.Key]
	}
	pb.impl.opcache[args.Client+":"+strconv.Itoa(args.SeqNo)] = Result{SeqNo: args.SeqNo, V: *reply}
	reply.Err = OK
	log.Println(reply)
	return nil
}

// server Push() RPC handler
func (pb *PBServer) Push(args PushArgs, reply *PushReply) error {
	log.Println("Push received")
	pb.impl.database = args.KVStore
	pb.impl.currentView = args.View
	log.Println("Received push, database is now: ", pb.impl.database)

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
		var reply PushReply
		log.Println("Pushing")
		ok := call(latestView.Backup, "PBServer.Push", PushArgs{KVStore: pb.impl.database, OpCache: pb.impl.opcache, View: latestView}, &reply)
		if !ok {
			log.Println("Push failed error ", reply.Err)
		}
	}
	pb.impl.currentView = latestView

}
