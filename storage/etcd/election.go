package etcd

import (
	"context"
	"fmt"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

// modified from: https://gist.github.com/thrawn01/c007e6a37b682d3899910e33243a3cdc

type Election struct {
	Client           *etcd.Client
	ElectionName     string
	CandidateName    string
	ResumeLeader     bool
	TTL              int
	ReconnectBackOff time.Duration
	session          *concurrency.Session
	election         *concurrency.Election
}

func (e *Election) Run(ctx context.Context) (<-chan bool, error) {
	var observe <-chan etcd.GetResponse
	var node *etcd.GetResponse
	var errChan chan error
	var isLeader bool
	var err error

	var leaderChan chan bool
	setLeader := func(set bool) {
		// Only report changes in leadership
		if isLeader == set {
			return
		}
		isLeader = set
		leaderChan <- set
	}

	if err = e.newSession(ctx, 0); err != nil {
		return nil, errors.Wrap(err, "while creating initial session")
	}

	go func() {
		leaderChan = make(chan bool, 10)
		defer close(leaderChan)

		for {
			// Discover who if any, is leader of this election
			if node, err = e.election.Leader(ctx); err != nil {
				if err != concurrency.ErrElectionNoLeader {
					plog.Error(fmt.Sprintf("Error encountered while determining election leader: %s", err))
					goto reconnect
				}
			} else {
				// If we are resuming an election from which we previously had leadership we
				// have 2 options
				// 1. Resume the leadership if the lease has not expired. This is a race as the
				//    lease could expire in between the `Leader()` call and when we resume
				//    observing changes to the election. If this happens we should detect the
				//    session has expired during the observation loop.
				// 2. Resign the leadership immediately to allow a new leader to be chosen.
				//    This option will almost always result in transfer of leadership.
				if string(node.Kvs[0].Value) == e.CandidateName {
					// If we want to resume leadership
					if e.ResumeLeader {
						// Recreate our session with the old lease id
						if err = e.newSession(ctx, node.Kvs[0].Lease); err != nil {
							plog.Error(fmt.Sprintf("Error encountered while re-establishing session with lease: %s", err))
							goto reconnect
						}
						e.election = concurrency.ResumeElection(e.session, e.ElectionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)

						// Because Campaign() only returns if the election entry doesn't exist
						// we must skip the campaign call and go directly to observe when resuming
						goto observe
					} else {
						// If resign takes longer than our e.TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(ctx, time.Duration(e.TTL))
						e.election = concurrency.ResumeElection(e.session, e.ElectionName,
							string(node.Kvs[0].Key), node.Kvs[0].CreateRevision)
						err = e.election.Resign(ctx)
						cancel()
						if err != nil {
							plog.Error(fmt.Sprintf("Error encountered while resigning leadership after reconnect: %s", err))
							goto reconnect
						}
					}
				}
			}
			// Reset leadership if we had it previously
			setLeader(false)

			// Attempt to become leader
			errChan = make(chan error)
			go func() {
				// Make this a non blocking call so we can check for session close
				errChan <- e.election.Campaign(ctx, e.CandidateName)
			}()

			select {
			case err = <-errChan:
				if err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					// NOTE: Campaign currently does not return an error if session expires
					plog.Error(fmt.Sprintf("Error encountered while campaigning for leader: %s", err))
					e.session.Close()
					goto reconnect
				}
			case <-ctx.Done():
				e.session.Close()
				return
			case <-e.session.Done():
				goto reconnect
			}

		observe:
			// If Campaign() returned without error, we are leader
			setLeader(true)

			// Observe changes to leadership
			observe = e.election.Observe(ctx)
			for {
				select {
				case resp, ok := <-observe:
					if !ok {
						// NOTE: Observe will not close if the session expires, we must
						// watch for session.Done()
						e.session.Close()
						goto reconnect
					}
					if string(resp.Kvs[0].Value) == e.CandidateName {
						setLeader(true)
					} else {
						// We are not leader
						setLeader(false)
						break
					}
				case <-ctx.Done():
					if isLeader {
						// If resign takes longer than our e.TTL then lease is expired and we are no
						// longer leader anyway.
						ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(e.TTL))
						if err = e.election.Resign(ctx); err != nil {
							plog.Error(fmt.Sprintf("Error encountered while resigning leadership during shutdown: %s", err))
						}
						cancel()
					}
					e.session.Close()
					return
				case <-e.session.Done():
					goto reconnect
				}
			}

		reconnect:
			setLeader(false)

			for {
				if err = e.newSession(ctx, 0); err != nil {
					if errors.Cause(err) == context.Canceled {
						return
					}
					plog.Error(fmt.Sprintf("Error encountered while creating new session: %s", err))
					tick := time.NewTicker(e.ReconnectBackOff)
					select {
					case <-ctx.Done():
						tick.Stop()
						return
					case <-tick.C:
						tick.Stop()
					}
					continue
				}
				break
			}
		}
	}()

	// Wait until we have a leader before returning
	for {
		resp, err := e.election.Leader(ctx)
		if err != nil {
			if err != concurrency.ErrElectionNoLeader {
				return nil, err
			}
			time.Sleep(time.Millisecond * 300)
			continue
		}
		// If we are not leader, notify the channel
		if string(resp.Kvs[0].Value) != e.CandidateName {
			leaderChan <- false
		}
		break
	}
	return leaderChan, nil
}

func (e *Election) newSession(ctx context.Context, id int64) error {
	var err error
	e.session, err = concurrency.NewSession(e.Client, concurrency.WithTTL(e.TTL),
		concurrency.WithContext(ctx), concurrency.WithLease(etcd.LeaseID(id)))
	if err != nil {
		return err
	}
	e.election = concurrency.NewElection(e.session, e.ElectionName)
	return nil
}
