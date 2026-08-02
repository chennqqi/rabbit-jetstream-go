package rabbitjetstream

import "testing"

func BenchmarkSchedulerOrderAndRecord(b *testing.B) {
	scheduler, err := NewScheduler(SchedulerConfig{MaxPriority: 9})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	order := make([]int, 0, 10)
	for i := 0; i < b.N; i++ {
		order = scheduler.OrderInto(order)
		scheduler.Record(order[0])
	}
}
