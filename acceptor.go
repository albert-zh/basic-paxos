package basic_paxos

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

// Acceptor
/*
	minProposal: the promise proposal number
*/

type Acceptor struct {
	lis            net.Listener
	id             int
	minProposal    int
	acceptedNumber int
	acceptedValue  interface{}

	learners []int
}

func NewAcceptor(id int, learner []int) *Acceptor {
	acceptor := &Acceptor{
		id:       id,
		learners: learner,
	}
	acceptor.server()
	return acceptor
}

func (a *Acceptor) Prepare(args *MsgArgs, reply *MsgReply) error {
	if args.Number > a.minProposal {
		a.minProposal = args.Number
		reply.Number = a.acceptedNumber
		reply.Value = a.acceptedValue
		reply.Ok = true
	} else {
		reply.Ok = false
	}
	return nil
}

func (a *Acceptor) Accept(args *MsgArgs, reply *MsgReply) error {
	if args.Number >= a.minProposal {
		a.minProposal = args.Number
		a.acceptedNumber = args.Number
		a.acceptedValue = args.Value
		reply.Ok = true

		for _, lid := range a.learners {
			go func(learner int) {
				addr := fmt.Sprintf("127.0.0.1:%d", learner)
				args.From = a.id
				args.To = learner
				resp := new(MsgReply)
				ok := call(addr, "Learner.Learn", args, resp)
				if !ok {
					return
				}
			}(lid)
		}
	} else {
		reply.Ok = false
	}

	return nil
}

func (a *Acceptor) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(a)
	addr := fmt.Sprintf(":%d", a.id)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error", e)
	}
	a.lis = l
	go func() {
		for {
			conn, err := a.lis.Accept()
			if err != nil {
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()
}

func (a *Acceptor) close() {
	a.lis.Close()
}
