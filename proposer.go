package basic_paxos

import "fmt"

// Proposer
/*
	id: use to specify server
	round: use to specify consensus round
	number: number of proposal
*/
type Proposer struct {
	id        int
	round     int
	number    int
	acceptors []int
}

func (p *Proposer) ProposalNumber() int {
	return p.round<<16 | p.id
}

func (p *Proposer) majority() int {
	return len(p.acceptors)/2 + 1
}

func (p *Proposer) propose(v interface{}) interface{} {
	p.round++
	p.number = p.ProposalNumber()

	// phase 1
	prepareCount := 0
	maxNumber := 0

	for _, aid := range p.acceptors {
		args := MsgArgs{
			Number: p.number,
			From:   p.id,
			To:     aid,
		}
		reply := new(MsgReply)
		err := call(fmt.Sprintf("127.0.0.1:%d", aid), "Acceptor.Prepare", args, reply)
		if !err {
			continue
		}

		if reply.Ok {
			prepareCount++
			if reply.Number > maxNumber {
				maxNumber = reply.Number
				v = reply.Value
			}
		}

		// 如果收到的回复超过了半数，结束第一阶段
		if prepareCount == p.majority() {
			break
		}
	}

	// phase 2
	acceptCount := 0
	if prepareCount >= p.majority() {
		for _, aid := range p.acceptors {
			args := MsgArgs{
				Number: p.number,
				Value:  v,
				From:   p.id,
				To:     aid,
			}
			reply := new(MsgReply)
			ok := call(fmt.Sprintf("127.0.0.1:%d", aid), "Acceptor.Accept", args, reply)
			if !ok {
				continue
			}

			if reply.Ok {
				acceptCount++
			}
		}
	}

	if acceptCount >= p.majority() {
		// 选择的提案的值
		return v
	}
	return nil
}
