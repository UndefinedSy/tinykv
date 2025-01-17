// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// Your Code Here
	Peers []uint64
	// Your Code Here
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		Peers:            c.peers,
	}

	rand.Seed(time.Now().UnixNano())

	return r
	// Your Code Here (2A).
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.votes = map[uint64]bool{}
	r.Lead = None
	r.votes = make(map[uint64]bool)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// (blocking op?)
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr := r.Prs[to]

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Term:    r.Term,

		Entries: r.RaftLog.entries[pr.Next:]...,
	}

	return true
	// Your Code Here (2A).
}

func (r *Raft) bcastAppend() {
	for _, peer := range r.Peers {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) appendEntry(es ...pb.Entry) bool {
	cur := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = cur + 1 + i
	}

	r.RaftLog.entries = append(r.RaftLog.entries, es...)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}

	r.send(msg)
	// Your Code Here (2A).
}

func (r *Raft) bcastHeartbeat() {
	for _, peer := range r.Peers {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// r.becomeCandidate()
			r.electionElapsed = 0
			r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
		}
	}
	// Your Code Here (2A).
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.Term = term
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes[r.Vote] = true
	fmt.Printf("Raft[%d] has become candidate.", r.id)
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	lastIndex := r.RaftLog.LastIndex() + 1
	for _, pr := range r.Prs {
		pr.Match = 0
		pr.Next = lastIndex
	}

	emptyEnt := pb.Entry{Data: nil}
	r.appendEntry(emptyEnt)
	r.bcastAppend()

	fmt.Printf("Raft[%d] has become leader.", r.id)
	// NOTE: Leader should propose a noop entry on its term
	// Your Code Here (2A).
}

func (r *Raft) checkCandidateUpToDate(term, index uint64) bool {
	if len(r.RaftLog.entries) == 0 {
		return true
	}

	lastEntry := r.RaftLog.entries[r.RaftLog.lastEntryIndex]

	if lastEntry.Term != term {
		return term > lastEntry.Term
	}

	return index > lastEntry.Index
}

func (r *Raft) hup() {
	r.becomeCandidate()
	for _, peer := range r.Peers {
		msg := pb.Message{
			From: r.id,
			To:   peer,
			Term: r.Term,
		}

		msg.Index = r.RaftLog.LastIndex()
		msg.LogTerm, _ = r.RaftLog.Term(msg.Index)

		switch peer {
		case r.id:
			msg.MsgType = pb.MessageType_MsgRequestVoteResponse
			r.Step(msg)
		default:
			msg.MsgType = pb.MessageType_MsgRequestVote
			msg.Index = r.RaftLog.LastIndex()
			msg.LogTerm, _ = r.RaftLog.Term(msg.Index)
			r.msgs = append(r.msgs, msg)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == None:
		// send by myself
	case m.Term > r.Term:
		switch m.MsgType {
		// case pb.MessageType_MsgRequestVote:
		// 	r.becomeFollower(m.Term, 0)
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, 0)
		}

	case m.Term < r.Term:
		// Might be a re-joined leader or staled something...
		if m.MsgType == pb.MessageType_MsgRequestVote {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
		}

		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
		}

		if (r.Vote == 0 || r.Vote == m.From) &&
			r.checkCandidateUpToDate(m.Term, m.Index) {
			msg.Reject = false
			r.Vote = m.From
			r.electionElapsed = 0
		} else {
			msg.Reject = true
		}

		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate && !m.Reject {
			r.votes[m.From] = true
			if r.hasQuorumVotes() {
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		fmt.Printf("Received heartbeat response from raft peer[%d]\n", m.From)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		fmt.Printf("Received append response from raft peer[%d]\n", m.From)
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	}
	// switch r.State {
	// case StateFollower:
	// case StateCandidate:
	// case StateLeader:
	// }

	// Your Code Here (2A).
	return nil
}

func (r *Raft) hasQuorumVotes() bool {
	votesCount := 0
	for _, vote := range r.votes {
		if vote {
			votesCount++
		}
	}
	return votesCount >= (len(r.Peers)+1)/2
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Lead != m.From {
		r.becomeFollower(m.Term, m.From)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
	}

	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	}
	r.msgs = append(r.msgs, msg)
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
