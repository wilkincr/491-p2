package viewservice

import (
	"fmt"
	"log"
)

// additions to ViewServer state.
type ViewServerImpl struct {
	currentView View
	// proxyMap     map[string]*ServerProxy
	primaryAcked bool
	adder        chan string
	resetter     chan string
	current      chan map[string]*ServerProxy
}

type ServerProxy struct {
	ID               string
	missedHeartbeats int
	alive            bool
}

// your vs.impl.* initializations here.
func (vs *ViewServer) initImpl() {
	vs.impl.currentView.Viewnum = 0
	vs.impl.currentView.Primary = ""
	vs.impl.currentView.Backup = ""
	vs.impl.primaryAcked = false
	vs.impl.adder = make(chan string)
	vs.impl.resetter = make(chan string)
	vs.impl.current = make(chan map[string]*ServerProxy)

	go func() {
		proxymap := make(map[string]*ServerProxy)
		for {
			select {
			case clientAddr := <-vs.impl.adder:
				if _, exists := proxymap[clientAddr]; !exists {
					proxymap[clientAddr] = &ServerProxy{ID: clientAddr, alive: true, missedHeartbeats: 0}
				}
			case clientAddr := <-vs.impl.resetter:
				proxymap[clientAddr].missedHeartbeats = 0
				proxymap[clientAddr].alive = true
			case vs.impl.current <- proxymap:
			}
		}
	}()
}

func (vs *ViewServer) add(clientAddr string) {
	vs.impl.adder <- clientAddr
}

func (vs *ViewServer) reset(clientAddr string) {
	vs.impl.resetter <- clientAddr
}

func (vs *ViewServer) get() map[string]*ServerProxy {
	return <-vs.impl.current
}

func (vs *ViewServer) IncrementView() {
	vs.impl.currentView.Viewnum++
	vs.impl.primaryAcked = false
}

func (vs *ViewServer) NeedBackup(clientAddr string) bool {
	return vs.impl.currentView.Backup == "" && vs.impl.currentView.Primary != "" && vs.impl.currentView.Primary != clientAddr
}

func (vs *ViewServer) PrimaryRestart(clientAddr string, viewnum uint) bool {
	return vs.impl.currentView.Primary == clientAddr && viewnum == 0 && vs.impl.currentView.Viewnum != 0
}

func (vs *ViewServer) PrimaryAck(clientAddr string, viewnum uint) bool {
	return vs.impl.currentView.Primary == clientAddr && vs.impl.currentView.Viewnum == viewnum
}

// Ping() RPC handler implementation
func (vs *ViewServer) PingImpl(args *PingArgs, reply *PingReply) error {
	clientAddr := args.Me
	clientViewnum := args.Viewnum
	// fmt.Println("ping from", clientAddr)
	// Checking for new client, only adds if doesn't exist
	vs.add(clientAddr)
	// First ping, add primary
	if vs.impl.currentView.Viewnum == 0 {
		// Add primary
		// fmt.Println("adding primary", clientAddr)
		vs.impl.currentView.Viewnum = 1
		vs.impl.currentView.Primary = clientAddr
	} else if vs.NeedBackup(clientAddr) {
		// Add backup
		// fmt.Println("adding backup", clientAddr)
		// fmt.Println(("Incrementing view number"))
		vs.IncrementView()
		vs.impl.currentView.Backup = clientAddr
	} else if vs.PrimaryRestart(clientAddr, clientViewnum) {
		// Primary restart
		// fmt.Println("primary restart")
		vs.handleFailure(clientAddr)
	} else if vs.PrimaryAck(clientAddr, clientViewnum) {
		vs.impl.primaryAcked = true
	}

	reply.View = vs.impl.currentView
	vs.reset(clientAddr)
	return nil
}

// Get() RPC handler implementation
func (vs *ViewServer) GetImpl(args *GetArgs, reply *GetReply) error {
	reply.View = vs.impl.currentView
	return nil
}

func (vs *ViewServer) idleAvailable(proxy *ServerProxy) bool {
	if proxy.ID != vs.impl.currentView.Primary &&
		proxy.ID != vs.impl.currentView.Backup &&
		proxy.alive {
		return true
	}
	return false
}

func (vs *ViewServer) handleFailure(proxyID string) {
	// Primary failed
	if proxyID == vs.impl.currentView.Primary {
		// Check if old primary hasn't acked
		if !vs.impl.primaryAcked {
			fmt.Println("Old primary hasn't acked")
			return
		}
		// Safe to promote backup to primary
		vs.impl.currentView.Primary = vs.impl.currentView.Backup
		log.Println("Promoting backup to primary: ", vs.impl.currentView.Primary)
		// No backup (for now)
		vs.impl.currentView.Backup = ""
		// Check for available idle servers
		for _, proxy := range <-vs.impl.current {
			fmt.Println(proxy.ID)
			fmt.Println(proxy.alive)
			if vs.idleAvailable(proxy) {
				// Promote idle to backup
				vs.impl.currentView.Backup = proxy.ID
				break
			}
		}
		vs.IncrementView()
	} else if proxyID == vs.impl.currentView.Backup {
		// Backup failed, no backup (for now)
		vs.impl.currentView.Backup = ""
		// Check for available idle servers
		for _, proxy := range vs.get() {
			if vs.idleAvailable(proxy) {
				// Promote idle to backup
				vs.impl.currentView.Backup = proxy.ID
				break
			}
		}
		vs.IncrementView()
	}

}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	for _, proxy := range vs.get() {
		if proxy.missedHeartbeats < DeadPings {
			proxy.missedHeartbeats++
		}

		if proxy.missedHeartbeats >= DeadPings {
			proxy.alive = false
			if proxy.ID == vs.impl.currentView.Primary ||
				proxy.ID == vs.impl.currentView.Backup {
				vs.handleFailure(proxy.ID)
			}
		}
	}

}
