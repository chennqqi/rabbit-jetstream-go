package rabbitjetstream

import (
	"reflect"
	"testing"
)

func TestSchedulerStrictPriorityAndBoundedProbe(t *testing.T) {
	s, err := NewScheduler(SchedulerConfig{MaxPriority: 3, HighPriorityBurst: 2, LowPriorityProbe: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.Order(), []int{3, 2, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial order = %v, want %v", got, want)
	}
	s.Record(3)
	s.Record(3)
	if got, want := s.Order(), []int{2, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("probe order = %v, want %v", got, want)
	}
	s.Record(1)
	if got, want := s.Order(), []int{3, 2, 1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("post-probe order = %v, want %v", got, want)
	}
}

func TestSchedulerEmptyProbeRestoresHighPriority(t *testing.T) {
	s, err := NewScheduler(SchedulerConfig{MaxPriority: 1, HighPriorityBurst: 1, LowPriorityProbe: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.Record(1)
	if got := s.Order(); !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("probe order = %v", got)
	}
	s.RecordEmpty()
	if got := s.Order(); !reflect.DeepEqual(got, []int{1, 0}) {
		t.Fatalf("restored order = %v", got)
	}
}

func TestSchedulerProbesBelowHighestActiveLevel(t *testing.T) {
	s, err := NewScheduler(SchedulerConfig{MaxPriority: 3, HighPriorityBurst: 2, LowPriorityProbe: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.Record(2)
	s.Record(2)
	if got, want := s.Order(), []int{1, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active-level probe order = %v, want %v", got, want)
	}
}

func TestSchedulerDefaultsAndValidation(t *testing.T) {
	s, err := NewScheduler(SchedulerConfig{MaxPriority: 0})
	if err != nil {
		t.Fatal(err)
	}
	if s.config.HighPriorityBurst != DefaultHighPriorityBurst || s.config.LowPriorityProbe != DefaultLowPriorityProbe {
		t.Fatalf("defaults = %+v", s.config)
	}
	if _, err := NewScheduler(SchedulerConfig{MaxPriority: 256}); err == nil {
		t.Fatal("expected invalid max priority")
	}
}
