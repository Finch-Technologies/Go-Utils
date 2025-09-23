package elector

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/finch-technologies/go-utils/database/dynamo"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"

	"github.com/google/uuid"
)

var (
	defaultMinDelay     = 30 * time.Second
	defaultMaxDelay     = 45 * time.Second
	defaultInterval     = 1 * time.Minute
	defaultLeaseTimeout = 2 * time.Minute
	defaultKeyName      = "proxyman_leader_lock"

	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
)

var elector *Elector

// ElectorConfig holds configuration for the leader elector
type ElectorConfig struct {
	TableName     string
	MinDelay      time.Duration
	MaxDelay      time.Duration
	CheckInterval time.Duration
	LeaseTimeout  time.Duration
	KeyName       string
}

// Elector handles leader election using a distributed lock
type Elector struct {
	instanceID     string
	config         ElectorConfig
	initialDelay   time.Duration
	initOnce       sync.Once
	ctx            context.Context
	cancel         context.CancelFunc
	electionTicker *time.Ticker
	mu             sync.RWMutex
	tableName      string
	isLeader       bool
}

// getDefaultConfig returns the default configuration
func getDefaultConfig() ElectorConfig {
	return ElectorConfig{
		TableName:     "default",
		MinDelay:      defaultMinDelay,
		MaxDelay:      defaultMaxDelay,
		CheckInterval: defaultInterval,
		LeaseTimeout:  defaultLeaseTimeout,
		KeyName:       defaultKeyName,
	}
}

func Start(opts ...ElectorConfig) error {
	defaultConfig := getDefaultConfig()

	cfg := defaultConfig

	// Apply all provided options
	if len(opts) > 0 {
		cfg = opts[0]
		//Merge the config with the default config
		utils.MergeObjects(&cfg, defaultConfig)
	}

	// Generate a unique instance ID
	instanceID := uuid.New().String()

	// Generate initial delay
	initialDelay := generateInitialDelay(cfg.MinDelay, cfg.MaxDelay)

	ctx, cancel := context.WithCancel(context.Background())

	elector = &Elector{
		isLeader:     false,
		instanceID:   instanceID,
		config:       cfg,
		initialDelay: initialDelay,
		ctx:          ctx,
		cancel:       cancel,
	}

	initialTimer := time.NewTimer(initialDelay)

	log.Debugf("Starting leader election with instance ID: %s", instanceID)
	log.Debugf("Random initial delay: %v", initialDelay)

	go func() {
		defer initialTimer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-initialTimer.C:
			initializeElection()
		}
	}()

	return nil
}

// initializeElection starts the periodic election process
func initializeElection() {
	elector.initOnce.Do(func() {
		log.Debugf("Initial delay completed, starting leader election process (delayed: %v)", elector.initialDelay)

		elector.electionTicker = time.NewTicker(elector.config.CheckInterval)

		// Run immediate election attempt instead of waiting for first tick
		go runElectionCycle()

		go func() {
			defer elector.electionTicker.Stop()

			for {
				select {
				case <-elector.ctx.Done():
					return
				case <-elector.electionTicker.C:
					runElectionCycle()
				}
			}
		}()
	})
}

// runElectionCycle performs a single election cycle
func runElectionCycle() {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		log.Debugf("Election cycle completed in %v", duration)
	}()

	if IsLeader() {
		log.Debug("Instance is leader, trying to renew leadership")
		success, err := renewLeadership()
		if err != nil {
			log.Errorf("Failed to renew leadership: %v", err)
			setLeader(false)
		} else if !success {
			log.Info("Lost leadership")
			setLeader(false)
		} else {
			log.Debug("Leadership renewed successfully")
		}
	} else {
		log.Debug("Instance is not leader, trying to acquire leadership")
		success, err := attemptLeadership()
		if err != nil {
			log.Errorf("Failed to attempt leadership: %v", err)
		} else if success {
			log.Info("Acquired leadership")
			setLeader(true)
		} else {
			log.Debug("Leadership attempt failed (another instance is leader)")
		}
	}
}

// attemptLeadership tries to acquire leadership
func attemptLeadership() (bool, error) {
	// Check context first
	select {
	case <-elector.ctx.Done():
		return false, context.Canceled
	default:
	}

	// Check if there's already a leader
	leader, err := getLeader()
	if err != nil {
		return false, fmt.Errorf("failed to check current leader: %w", err)
	}

	// If there's already a leader, don't attempt to acquire
	if leader != "" {
		return false, nil
	}

	// Try to set ourselves as the leader
	err = dynamo.Put(elector.config.TableName, elector.config.KeyName, elector.instanceID, dynamo.PutOptions{
		Ttl: elector.config.LeaseTimeout,
	})
	if err != nil {
		return false, fmt.Errorf("failed to set leadership: %w", err)
	}

	// Verify that we actually became the leader
	currentLeader, err := getLeader()
	if err != nil {
		return false, fmt.Errorf("failed to verify leadership: %w", err)
	}

	return currentLeader == elector.instanceID, nil
}

// renewLeadership renews the leadership lease if still the leader
func renewLeadership() (bool, error) {
	// Check context first
	select {
	case <-elector.ctx.Done():
		return false, context.Canceled
	default:
	}

	// Check if we're still the leader
	leader, err := getLeader()
	if err != nil {
		return false, fmt.Errorf("failed to get leader: %w", err)
	}

	if leader != elector.instanceID {
		return false, nil
	}

	// Renew our lease
	err = dynamo.Put(elector.config.TableName, elector.config.KeyName, elector.instanceID, dynamo.PutOptions{
		Ttl: elector.config.LeaseTimeout,
	})
	if err != nil {
		return false, fmt.Errorf("failed to renew lease: %w", err)
	}

	return true, nil
}

// revokeLeadership releases leadership
func revokeLeadership() error {
	if !IsLeader() {
		log.Debug("Not the leader anymore, no need to revoke")
		return nil // Nothing to revoke
	}

	log.Debug("Revoking leadership for instance: %s", elector.instanceID)

	// First, verify we're still the leader by checking the database
	currentLeader, err := getLeader()
	if err != nil {
		log.Warningf("Failed to verify current leader before revocation: %v", err)
		// Continue with revocation anyway
	} else if currentLeader != elector.instanceID {
		log.Warningf("Not the current leader anymore (current: %s, me: %s), skipping revocation", currentLeader, elector.instanceID)
		setLeader(false)
		return nil
	}

	// Delete the leader lock
	err = dynamo.Delete(elector.config.TableName, elector.config.KeyName)

	if err != nil {
		return fmt.Errorf("failed to revoke leadership: %w", err)
	}

	setLeader(false)

	// Verify the deletion worked
	verifyLeader, err := getLeader()
	if err != nil {
		log.Warningf("Failed to verify leadership revocation: %v", err)
	} else if verifyLeader != "" {
		log.Warningf("Leadership revocation may have failed - leader still exists: %s", verifyLeader)
	} else {
		log.Debug("Leadership revoked successfully - confirmed empty")
	}

	return nil
}

// getLeader retrieves the current leader from the distributed lock
func getLeader() (string, error) {
	// Check if context is cancelled before making database call
	// select {
	// case <-elector.ctx.Done():
	// 	return "", elector.ctx.Err()
	// default:
	// }

	// Query the database directly (no cache)
	result, _, err := dynamo.GetString(elector.config.TableName, elector.config.KeyName)

	if err != nil {
		// Check if context was cancelled during the operation
		select {
		case <-elector.ctx.Done():
			return "", elector.ctx.Err()
		default:
		}
		return "", fmt.Errorf("failed to get leader from store: %w", err)
	}

	return result, nil
}

// setLeader updates the leader status
func setLeader(isLeader bool) {
	elector.mu.Lock()
	defer elector.mu.Unlock()

	oldStatus := elector.isLeader
	elector.isLeader = isLeader

	// Log status changes
	if oldStatus != isLeader {
		if isLeader {
			log.Debugf("Instance %s became leader", elector.instanceID)
		} else {
			log.Debugf("Instance %s lost leadership", elector.instanceID)
		}
	}
}

func Stop() {
	log.Info("Stopping leader elector...")

	// First cancel the context to signal all goroutines to stop
	if elector.cancel != nil {
		elector.cancel()
	}

	// Stop the ticker immediately
	if elector.electionTicker != nil {
		elector.electionTicker.Stop()
	}

	// Try to revoke leadership if we're the leader
	if IsLeader() {
		if err := revokeLeadership(); err != nil {
			log.Debugf("Failed to revoke leadership during shutdown: %v", err)
		}
	}

	log.Info("Leader elector stopped")
}

func IsLeader() bool {
	elector.mu.RLock()
	defer elector.mu.RUnlock()
	return elector.isLeader
}

// Generate random initial delay between min and max duration
func generateInitialDelay(minDelay, maxDelay time.Duration) time.Duration {
	minMs := minDelay.Milliseconds()
	maxMs := maxDelay.Milliseconds()
	delayRange := maxMs - minMs
	if delayRange <= 0 {
		return minDelay
	}
	return minDelay + time.Duration(rng.Int63n(delayRange))*time.Millisecond
}

// GetInstanceID returns the unique instance ID
func GetInstanceID() string {
	return elector.instanceID
}
