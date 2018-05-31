// Copyright (c) 2018 IoTeX
// This is an alpha (internal) release and is not suitable for production. This source code is provided ‘as is’ and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package rolldpos

import (
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/iotexproject/iotex-core/blockchain"
	"github.com/iotexproject/iotex-core/common"
	"github.com/iotexproject/iotex-core/common/routine"
	"github.com/iotexproject/iotex-core/config"
	"github.com/iotexproject/iotex-core/consensus/fsm"
	"github.com/iotexproject/iotex-core/consensus/scheme"
	"github.com/iotexproject/iotex-core/delegate"
	"github.com/iotexproject/iotex-core/logger"
	pb "github.com/iotexproject/iotex-core/proto"
)

var (
	// ErrInvalidViewChangeMsg is the error that ViewChangeMsg is invalid
	ErrInvalidViewChangeMsg = errors.New("ViewChangeMsg is invalid")
)

// roundCtx keeps the context data for the current round and block.
type roundCtx struct {
	block     *blockchain.Block
	blockHash *common.Hash32B
	prevotes  map[net.Addr]*common.Hash32B
	votes     map[net.Addr]*common.Hash32B
	isPr      bool
	delegates []net.Addr
}

// DNet is the delegate networks interface.
type DNet interface {
	Tell(node net.Addr, msg proto.Message) error
	Self() net.Addr
	Broadcast(msg proto.Message) error
}

// rollDPoSCB contains all the callback functions used in RollDPoS
type rollDPoSCB struct {
	propCb scheme.CreateBlockCB
	voteCb scheme.TellPeerCB
	consCb scheme.ConsensusDoneCB
	pubCb  scheme.BroadcastCB
	prCb   scheme.GetProposerCB
}

// RollDPoS is the RollDPoS consensus scheme
type RollDPoS struct {
	rollDPoSCB
	bc        blockchain.Blockchain
	fsm       fsm.Machine
	roundCtx  *roundCtx
	self      net.Addr
	pool      delegate.Pool
	wg        sync.WaitGroup
	quit      chan struct{}
	eventChan chan *fsm.Event
	cfg       config.RollDPoS
	pr        *routine.RecurringTask
	prnd      *proposerRotation
	done      chan bool
}

// NewRollDPoS creates a RollDPoS struct
func NewRollDPoS(
	cfg config.RollDPoS,
	prop scheme.CreateBlockCB,
	vote scheme.TellPeerCB,
	cons scheme.ConsensusDoneCB,
	pub scheme.BroadcastCB,
	pr scheme.GetProposerCB,
	bc blockchain.Blockchain,
	myaddr net.Addr,
	dlg delegate.Pool) *RollDPoS {
	cb := rollDPoSCB{
		propCb: prop,
		voteCb: vote,
		consCb: cons,
		pubCb:  pub,
		prCb:   pr,
	}
	sc := &RollDPoS{
		rollDPoSCB: cb,
		bc:         bc,
		self:       myaddr,
		pool:       dlg,
		quit:       make(chan struct{}),
		eventChan:  make(chan *fsm.Event, 100),
		cfg:        cfg,
	}
	if int(cfg.ProposerRotation.Interval) == 0 {
		sc.prnd = newProposerRotationNoDelay(sc)
	} else {
		sc.pr = newProposerRotation(sc)
	}
	sc.fsm = fsmCreate(sc)
	return sc
}

// Start initialize the RollDPoS and start to consume requests from request channel.
func (n *RollDPoS) Start() error {
	logger.Info().Msg("Starting RollDPoS")

	n.wg.Add(1)
	go n.consume()
	if int(n.cfg.ProposerRotation.Interval) == 0 {
		n.prnd.Do()
	} else if n.cfg.ProposerRotation.Enabled {
		n.pr.Start()
	}
	return nil
}

// Stop stops the RollDPoS and stop consuming requests from request channel.
func (n *RollDPoS) Stop() error {
	logger.Info().Msg("RollDPoS is shutting down")
	close(n.quit)
	n.wg.Wait()
	return nil
}

// SetDoneStream sets a boolean channel which indicates to the simulator that the consensus is done
func (n *RollDPoS) SetDoneStream(done chan bool) {
	n.done = done
}

// Handle handles incoming messages and publish to the channel.
func (n *RollDPoS) Handle(m proto.Message) error {
	logger.Debug().Msg("RollDPoS scheme handles incoming requests")

	event, err := eventFromProto(m)
	if err != nil {
		return err
	}

	n.eventChan <- event
	return nil
}

func (n *RollDPoS) consume() {
loop:
	for {
		select {
		case r := <-n.eventChan:
			err := n.fsm.HandleTransition(r)
			if err == nil {
				break
			}

			fErr := errors.Cause(err)
			switch fErr {
			case fsm.ErrStateHandlerNotMatched:
				// if fsm state has not changed since message was last seen, write to done channel
				if n.fsm.CurrentState() == r.SeenState && n.done != nil {
					select {
					case n.done <- true: // try to write to done if possible
					default:
					}
				}
				r.SeenState = n.fsm.CurrentState()

				if r.ExpireAt == nil {
					expireAt := time.Now().Add(n.cfg.UnmatchedEventTTL)
					r.ExpireAt = &expireAt
					n.eventChan <- r
				} else if time.Now().Before(*r.ExpireAt) {
					n.eventChan <- r
				}
			case fsm.ErrNoTransitionApplied:
			default:
				logger.Error().
					Str("RollDPoS", n.self.String()).
					Err(err).
					Msg("Failed to fsm.HandleTransition")
			}
		case <-n.quit:
			break loop
		default:
			// if there are no events, try to write to done channel
			if n.done != nil {
				select {
				case n.done <- true: // try to write to done if possible
				default:
				}
			}
		}
	}

	n.wg.Done()
	logger.Info().Msg("consume done")
}

func (n *RollDPoS) tellDelegates(msg *pb.ViewChangeMsg) {
	msg.SenderAddr = n.self.String()
	n.voteCb(msg)
}
