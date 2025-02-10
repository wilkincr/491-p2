package viewservice

import (
	"fmt"
	"log"
)

// additions to ViewServer state.
type ViewServerImpl struct {
	currentView  View
	proxyMap     map[string]*ServerProxy
	primaryAcked bool
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
	vs.impl.proxyMap = make(map[string]*ServerProxy)
	vs.impl.primaryAcked = false
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
	// Checking for new client
	if _, exists := vs.impl.proxyMap[clientAddr]; !exists {
		vs.impl.proxyMap[clientAddr] = &ServerProxy{ID: clientAddr, alive: true, missedHeartbeats: 0}
	}
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
	vs.impl.proxyMap[clientAddr].missedHeartbeats = 0
	vs.impl.proxyMap[clientAddr].alive = true
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
			fmt.Println("No good!")
			return
		}
		// Safe to promote backup to primary
		vs.impl.currentView.Primary = vs.impl.currentView.Backup
		log.Println("Promoting backup to primary: ", vs.impl.currentView.Primary)
		// No backup (for now)
		vs.impl.currentView.Backup = ""
		// Check for available idle servers
		for _, proxy := range vs.impl.proxyMap {
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
		for _, proxy := range vs.impl.proxyMap {
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
	for _, proxy := range vs.impl.proxyMap {
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
