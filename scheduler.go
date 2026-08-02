package rabbitjetstream

import "fmt"

const (
	DefaultHighPriorityBurst = 64
	DefaultLowPriorityProbe  = 1
)

type SchedulerConfig struct {
	MaxPriority       int
	HighPriorityBurst int
	LowPriorityProbe  int
}

func (c SchedulerConfig) validate() error {
	if err := ValidatePriority(c.MaxPriority); err != nil {
		return fmt.Errorf("max priority: %w", err)
	}
	if c.HighPriorityBurst <= 0 {
		return fmt.Errorf("high priority burst must be positive")
	}
	if c.LowPriorityProbe <= 0 {
		return fmt.Errorf("low priority probe must be positive")
	}
	return nil
}

// Scheduler implements bounded strict priority. It normally scans from the
// highest priority down. After a bounded number of high-priority deliveries it
// excludes that level for LowPriorityProbe successful lower-level deliveries.
type Scheduler struct {
	config         SchedulerConfig
	burstPriority  int
	highDeliveries int
	probeRemaining int
	probeCeiling   int
}

func NewScheduler(config SchedulerConfig) (*Scheduler, error) {
	if config.HighPriorityBurst == 0 {
		config.HighPriorityBurst = DefaultHighPriorityBurst
	}
	if config.LowPriorityProbe == 0 {
		config.LowPriorityProbe = DefaultLowPriorityProbe
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Scheduler{config: config, burstPriority: -1, probeCeiling: -1}, nil
}

// Order returns the priority levels to probe for the next delivery.
func (s *Scheduler) Order() []int {
	start := s.config.MaxPriority
	if s.probeRemaining > 0 {
		start = s.probeCeiling
	}
	if start < MinPriority {
		return nil
	}
	order := make([]int, 0, start-MinPriority+1)
	for priority := start; priority >= MinPriority; priority-- {
		order = append(order, priority)
	}
	return order
}

// Record records the selected priority. Call it only after a message is found.
func (s *Scheduler) Record(priority int) {
	if s.probeRemaining > 0 {
		s.probeRemaining--
		s.highDeliveries = 0
		s.burstPriority = -1
		return
	}
	if priority > MinPriority {
		if priority != s.burstPriority {
			s.burstPriority = priority
			s.highDeliveries = 0
		}
		s.highDeliveries++
		if s.highDeliveries >= s.config.HighPriorityBurst {
			s.probeRemaining = s.config.LowPriorityProbe
			s.probeCeiling = priority - 1
		}
		return
	}
	s.highDeliveries = 0
	s.burstPriority = -1
}

// RecordEmpty completes a forced lower-level probe when all lower levels are
// empty, so an empty low-priority tier cannot stall high-priority traffic.
func (s *Scheduler) RecordEmpty() {
	if s.probeRemaining > 0 {
		s.probeRemaining = 0
		s.highDeliveries = 0
		s.burstPriority = -1
	}
}
