package asynq

import (
	"crypto/sha256"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/teambition/rrule-go"
	"sort"
	"sync"
	"time"
)

// IcsTaskConfigProvider provides configs for periodic tasks.
// GetConfigs will be called by a IcsTaskManager periodically to
// sync the scheduler's entries with the configs returned by the provider.
type IcsTaskConfigProvider interface {
	GetConfigs() ([]*IcsTaskConfig, error)
}

type IcsTaskManager struct {
	s            *Scheduler
	p            IcsTaskConfigProvider
	syncInterval time.Duration
	done         chan (struct{})
	wg           sync.WaitGroup
	m            map[string]IcsTaskConfigIdentities // map[hash]entryID
}

type IcsTaskManagerOpts struct {
	// Required: must be non nil
	IcsTaskConfigProvider IcsTaskConfigProvider

	// Optional: if RedisUniversalClient is nil must be non nil
	RedisConnOpt RedisConnOpt

	// Optional: if RedisUniversalClient is non nil, RedisConnOpt is ignored.
	RedisUniversalClient redis.UniversalClient

	// Optional: scheduler options
	*SchedulerOpts

	// Optional: default is 3m
	SyncInterval time.Duration
}

const defaultIcsSyncInterval = 3 * time.Minute

type IcsEvent struct {
	StartDatetime time.Time

	EndDatetime time.Time

	// Optional: default nil
	RRule *rrule.RRule
}

type IcsTaskConfig struct {
	Event IcsEvent
	Task  *Task
	Opts  []Option
}

type IcsTaskConfigIdentities struct {
	entryID string
	taskID  string
}

func NewIcsTaskConfig(icsEvent IcsEvent, task *Task, opts []Option) *IcsTaskConfig {
	config := &IcsTaskConfig{
		Event: icsEvent,
		Task:  task,
		Opts:  opts,
	}

	timeout := Timeout(config.deadlineTimeout())
	config.Opts = append(config.Opts, timeout)
	return config
}

func (c *IcsTaskConfig) hash() string {
	h := sha256.New()
	_, _ = h.Write([]byte(c.Task.Type()))
	h.Write(c.Task.Payload())
	opts := stringifyOptions(c.Opts)
	sort.Strings(opts)

	if c.Event.RRule != nil {
		_, _ = h.Write([]byte(c.Event.RRule.String()))
	}

	for _, opt := range opts {
		_, _ = h.Write([]byte(opt))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (c *IcsTaskConfig) next() time.Time {
	if c.Event.RRule != nil {
		return c.Event.RRule.After(time.Now(), true)
	}
	if time.Now().Round(time.Second).Before(c.Event.StartDatetime) {
		return c.Event.StartDatetime
	} else {
		return time.Time{}
	}

}

func (c *IcsTaskConfig) nextCronspec() string {
	nextTime := c.next()
	if nextTime.IsZero() {
		return ""
	}
	return fmt.Sprintf("%v %v %v %v *", nextTime.Minute(), nextTime.Hour(), nextTime.Day(), int(nextTime.Month()))
}

// Return sub of event EndDatetime and StartDatetime by seconds
func (c *IcsTaskConfig) deadlineTimeout() time.Duration {
	return time.Duration(c.Event.EndDatetime.Sub(c.Event.StartDatetime).Seconds()) * time.Second
}

func NewIcsTaskManager(opts IcsTaskManagerOpts) (*IcsTaskManager, error) {
	if opts.IcsTaskConfigProvider == nil {
		return nil, fmt.Errorf("IcsTaskConfigProvider cannot be nil")
	}
	if opts.RedisConnOpt == nil && opts.RedisUniversalClient == nil {
		return nil, fmt.Errorf("RedisConnOpt/RedisUniversalClient cannot be nil")
	}
	var scheduler *Scheduler
	if opts.RedisUniversalClient != nil {
		scheduler = NewSchedulerFromRedisClient(opts.RedisUniversalClient, opts.SchedulerOpts)
	} else {
		scheduler = NewScheduler(opts.RedisConnOpt, opts.SchedulerOpts)
	}

	syncInterval := opts.SyncInterval
	if syncInterval == 0 {
		syncInterval = defaultIcsSyncInterval
	}
	return &IcsTaskManager{
		s:            scheduler,
		p:            opts.IcsTaskConfigProvider,
		syncInterval: syncInterval,
		done:         make(chan struct{}),
		m:            make(map[string]IcsTaskConfigIdentities),
	}, nil
}

func validateIcsTaskConfig(c *IcsTaskConfig) error {
	if c == nil {
		return fmt.Errorf("IcsTaskConfig cannot be nil")
	}
	if c.Task == nil {
		return fmt.Errorf("IcsTaskConfig.Task cannot be nil")
	}
	return nil
}

func (mgr *IcsTaskManager) Start() error {
	if mgr.s == nil || mgr.p == nil {
		panic("asynq: cannot start uninitialized IcsTaskManager; use NewIcsTaskManager to initialize")
	}
	if err := mgr.initialSync(); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	if err := mgr.s.Start(); err != nil {
		return fmt.Errorf("asynq: %v", err)
	}
	mgr.wg.Add(1)
	go func() {
		defer mgr.wg.Done()
		ticker := time.NewTicker(mgr.syncInterval)
		for {
			select {
			case <-mgr.done:
				mgr.s.logger.Debugf("Stopping syncer goroutine")
				ticker.Stop()
				return
			case <-ticker.C:
				mgr.sync()
			}
		}
	}()
	return nil
}

// Shutdown gracefully shuts down the manager.
// It notifies a background syncer goroutine to stop and stops scheduler.
func (mgr *IcsTaskManager) Shutdown() {
	close(mgr.done)
	mgr.wg.Wait()
	mgr.s.Shutdown()
}

// Run starts the manager and blocks until an os signal to exit the program is received.
// Once it receives a signal, it gracefully shuts down the manager.
func (mgr *IcsTaskManager) Run() error {
	if err := mgr.Start(); err != nil {
		return err
	}
	mgr.s.waitForSignals()
	mgr.Shutdown()
	mgr.s.logger.Debugf("IcsTaskManager exiting")
	return nil
}

func (mgr *IcsTaskManager) initialSync() error {
	configs, err := mgr.p.GetConfigs()
	if err != nil {
		return fmt.Errorf("initial call to GetConfigs failed: %v", err)
	}
	mgr.add(configs)
	return nil
}

func (mgr *IcsTaskManager) add(configs []*IcsTaskConfig) {
	for _, c := range configs {
		cronspec := c.nextCronspec()
		if cronspec == "" {
			continue
		}
		entryID, err := mgr.s.Register(cronspec, c.Task, c.Opts...)
		if err != nil {
			mgr.s.logger.Errorf("Failed to register periodic task: cronspec=%q task=%q err=%v",
				cronspec, c.Task.Type(), err)
			continue
		}
		mgr.m[c.hash()] = IcsTaskConfigIdentities{entryID: entryID, taskID: ""}

		for _, opt := range c.Opts {
			switch opt.(type) {
			case taskIDOption:
				if identities, ok := mgr.m[c.hash()]; ok {
					identities.taskID = opt.String()
					mgr.m[c.hash()] = identities
				}
			default:
				mgr.s.logger.Errorf("Task not set taskIdOption")
			}
		}
		mgr.s.logger.Infof("Successfully registered periodic task: cronspec=%q task=%q, entryID=%s",
			cronspec, c.Task.Type(), entryID)
	}
}

func (mgr *IcsTaskManager) remove(removed map[string]IcsTaskConfigIdentities) {
	for hash, identities := range removed {
		cancelationErr := mgr.s.rdb.PublishCancelation(identities.taskID)
		unregisterErr := mgr.s.Unregister(identities.entryID)

		if unregisterErr != nil && cancelationErr != nil {
			mgr.s.logger.Errorf(
				"Unregister periodic task or cancelation err=%v %v : entryId:%s, taskId:%s",
				unregisterErr,
				cancelationErr,
				identities.entryID,
				identities.taskID,
			)
			continue
		}

		delete(mgr.m, hash)
		mgr.s.logger.Infof("Successfully unregistered periodic task: entryID=%s", identities.entryID)
	}
}

func (mgr *IcsTaskManager) sync() {
	configs, err := mgr.p.GetConfigs()
	if err != nil {
		mgr.s.logger.Errorf("Failed to get periodic task configs: %v", err)
		return
	}
	for _, c := range configs {
		if err := validateIcsTaskConfig(c); err != nil {
			mgr.s.logger.Errorf("Failed to sync: GetConfigs returned an invalid config: %v", err)
			return
		}
	}
	// Diff and only register/unregister the newly added/removed entries.
	removed := mgr.diffRemoved(configs)
	added := mgr.diffAdded(configs)
	mgr.remove(removed)
	mgr.add(added)
}

// diffRemoved diffs the incoming configs with the registered config and returns
// a map containing hash and entryID of each config that was removed.
func (mgr *IcsTaskManager) diffRemoved(configs []*IcsTaskConfig) map[string]IcsTaskConfigIdentities {
	newConfigs := make(map[string]IcsTaskConfigIdentities)
	for _, c := range configs {
		newConfigs[c.hash()] = IcsTaskConfigIdentities{} // empty value since we don't have entryID yet
	}
	removed := make(map[string]IcsTaskConfigIdentities)
	for k, v := range mgr.m {
		// test whether existing config is present in the incoming configs
		if _, found := newConfigs[k]; !found {
			removed[k] = v
		}
	}
	return removed
}

// diffAdded diffs the incoming configs with the registered configs and returns
// a list of configs that were added.
func (mgr *IcsTaskManager) diffAdded(configs []*IcsTaskConfig) []*IcsTaskConfig {
	var added []*IcsTaskConfig
	for _, c := range configs {
		if _, found := mgr.m[c.hash()]; !found {
			added = append(added, c)
		}
	}
	return added
}
