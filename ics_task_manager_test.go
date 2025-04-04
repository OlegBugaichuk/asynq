package asynq

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/teambition/rrule-go"
	"sync"
	"testing"
	"time"
)

// Trivial implementation of IcsTaskConfigProvider for testing purpose.
type FakeIcsConfigProvider struct {
	mu   sync.Mutex
	cfgs []*IcsTaskConfig
}

func (p *FakeIcsConfigProvider) SetConfigs(cfgs []*IcsTaskConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfgs = cfgs
}

func (p *FakeIcsConfigProvider) GetConfigs() ([]*IcsTaskConfig, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfgs, nil
}

func TestIcsTaskConfigNext(t *testing.T) {
	currentDT := time.Now().Round(time.Second)
	nextDay := currentDT.AddDate(0, 0, 1)
	pastDay := currentDT.AddDate(0, 0, -1)
	next5Minute := currentDT.Add(time.Minute * 5).Round(time.Second)
	next10Minute := currentDT.Add(time.Minute * 10).Round(time.Second)

	r1, _ := rrule.NewRRule(rrule.ROption{
		Freq:     rrule.MINUTELY,
		Interval: 5,
		Dtstart:  currentDT,
	})

	r2, _ := rrule.NewRRule(rrule.ROption{
		Freq:     rrule.MINUTELY,
		Interval: 10,
		Dtstart:  currentDT,
	})

	// Since next checks against the current time,
	//it may slip when rounded to a second, so a slight delay is needed in tests.
	time.Sleep(time.Second)

	tests := []struct {
		desc string
		task *IcsTaskConfig
		next time.Time
	}{
		{
			desc: "Task without RRules",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: nextDay,
					EndDatetime:   nextDay,
				},
				Task: NewTask("foo", nil),
			},
			next: nextDay,
		},
		{
			desc: "Task with RRules 5 minutes",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: currentDT,
					EndDatetime:   nextDay,
					RRule:         r1,
				},
				Task: NewTask("foo", nil),
			},
			next: next5Minute,
		},
		{
			desc: "Task with RRules 10 minutes",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: currentDT,
					EndDatetime:   nextDay,
					RRule:         r2,
				},
				Task: NewTask("foo", nil),
			},
			next: next10Minute,
		},
		{
			desc: "Task without RRules",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: pastDay,
					EndDatetime:   pastDay,
				},
				Task: NewTask("foo", nil),
			},
			next: time.Time{},
		},
	}

	for _, test := range tests {
		if test.task.next() != test.next {
			t.Errorf("Task next: want %v, got %v", test.next, test.task.next())
		}
	}
}

func TestIcsTaskConfigNextCronspec(t *testing.T) {
	currentDT := time.Now().Round(time.Second)
	nextDay := currentDT.AddDate(0, 0, 1)
	pastDay := currentDT.AddDate(0, 0, -1)
	next5Minute := currentDT.Add(time.Minute * 5).Round(time.Second)
	next10Minute := currentDT.Add(time.Minute * 10).Round(time.Second)

	r1, _ := rrule.NewRRule(rrule.ROption{
		Freq:     rrule.MINUTELY,
		Interval: 5,
		Dtstart:  currentDT,
	})

	r2, _ := rrule.NewRRule(rrule.ROption{
		Freq:     rrule.MINUTELY,
		Interval: 10,
		Dtstart:  currentDT,
	})

	// Since next checks against the current time,
	//it may slip when rounded to a second, so a slight delay is needed in tests.
	time.Sleep(time.Second)

	tests := []struct {
		desc         string
		task         *IcsTaskConfig
		next         time.Time
		nextCronspec string
	}{
		{
			desc: "Task without RRules",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: nextDay,
					EndDatetime:   nextDay,
				},
				Task: NewTask("foo", nil),
			},
			next:         nextDay,
			nextCronspec: fmt.Sprintf("%v %v %v %v *", nextDay.Minute(), nextDay.Hour(), nextDay.Day(), int(nextDay.Month())),
		},
		{
			desc: "Task with RRules 5 minutes",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: currentDT,
					EndDatetime:   nextDay,
					RRule:         r1,
				},
				Task: NewTask("foo", nil),
			},
			next:         next5Minute,
			nextCronspec: fmt.Sprintf("%v %v %v %v *", next5Minute.Minute(), next5Minute.Hour(), next5Minute.Day(), int(next5Minute.Month())),
		},
		{
			desc: "Task with RRules 10 minutes",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: currentDT,
					EndDatetime:   nextDay,
					RRule:         r2,
				},
				Task: NewTask("foo", nil),
			},
			next:         next10Minute,
			nextCronspec: fmt.Sprintf("%v %v %v %v *", next10Minute.Minute(), next10Minute.Hour(), next10Minute.Day(), int(next10Minute.Month())),
		},
		{
			desc: "Task without RRules",
			task: &IcsTaskConfig{
				Event: IcsEvent{
					StartDatetime: pastDay,
					EndDatetime:   pastDay,
				},
				Task: NewTask("foo", nil),
			},
			next:         time.Time{},
			nextCronspec: "",
		},
	}

	for _, test := range tests {
		if test.task.nextCronspec() != test.nextCronspec {
			t.Errorf("Task next: want %v, got %v", test.nextCronspec, test.task.nextCronspec())
		}
	}
}

// Things to test.
// - Run the manager
// - Change provider to return new configs
// - Verify that the scheduler synced with the new config
func TestIcsTaskManager(t *testing.T) {
	// Note: In this test, we'll use task type as an ID for each config.
	currentDT := time.Now().Round(time.Second)
	nextDT := currentDT.AddDate(0, 0, 1)
	nextDTDelta2 := currentDT.AddDate(0, 0, 2)

	cfgs := []*IcsTaskConfig{
		{
			Event: IcsEvent{StartDatetime: nextDT, EndDatetime: nextDTDelta2},
			Task:  NewTask("task1", nil),
		},
		{
			Event: IcsEvent{StartDatetime: nextDT, EndDatetime: nextDTDelta2},
			Task:  NewTask("task2", nil),
		},
	}
	const syncInterval = 3 * time.Second
	provider := &FakeIcsConfigProvider{cfgs: cfgs}
	mgr, err := NewIcsTaskManager(IcsTaskManagerOpts{
		RedisConnOpt:          getRedisConnOpt(t),
		IcsTaskConfigProvider: provider,
		SyncInterval:          syncInterval,
	})
	if err != nil {
		t.Fatalf("Failed to initialize IcsTaskManager: %v", err)
	}

	if err := mgr.Start(); err != nil {
		t.Fatalf("Failed to start IcsTaskManager: %v", err)
	}
	defer mgr.Shutdown()

	got := extractCronEntries(mgr.s)
	var want []*cronEntry
	for _, cfg := range cfgs {
		want = append(want, &cronEntry{
			Cronspec: cfg.nextCronspec(),
			TaskType: cfg.Task.Type(),
		})
	}
	if diff := cmp.Diff(want, got, sortCronEntry); diff != "" {
		t.Errorf("Diff found in scheduler's registered entries: %s", diff)
	}

	// Change the underlying configs
	// All configs removed, empty set.
	provider.SetConfigs([]*IcsTaskConfig{})

	// Wait for the next sync
	time.Sleep(syncInterval * 2)

	// Verify the entries are synced
	got = extractCronEntries(mgr.s)
	want = []*cronEntry{}
	if diff := cmp.Diff(want, got, sortCronEntry); diff != "" {
		t.Errorf("Diff found in scheduler's registered entries: %s", diff)
	}
}

func TestIcsTaskConfig(t *testing.T) {
	// Note: In this test, we'll use task type as an ID for each config.
	currentDT := time.Now().Round(time.Second)
	nextDT := currentDT.AddDate(0, 0, 1)
	nextDTDelta2 := currentDT.AddDate(0, 0, 2)

	cfg := NewIcsTaskConfig(
		IcsEvent{StartDatetime: nextDT, EndDatetime: nextDTDelta2},
		NewTask("task1", nil),
		[]Option{},
	)
	opts, _ := composeOptions(cfg.Opts...)
	if opts.timeout != cfg.deadlineTimeout() {
		t.Errorf(
			"New task config timeout option (%x) not correct %x",
			opts.timeout,
			cfg.deadlineTimeout(),
		)
	}

}
