package rabbitjetstream

import "testing"

func TestContractNames(t *testing.T) {
	t.Parallel()
	stream, err := StreamName("orders_eu-1")
	if err != nil || stream != "RJSQ_orders_eu-1" {
		t.Fatalf("StreamName() = %q, %v", stream, err)
	}
	subject, err := PrioritySubject("orders", 7)
	if err != nil || subject != "rjs.q.orders.p.7" {
		t.Fatalf("PrioritySubject() = %q, %v", subject, err)
	}
	consumer, err := PriorityConsumerName("orders", 7)
	if err != nil || consumer != "RJSQC_orders_P7" {
		t.Fatalf("PriorityConsumerName() = %q, %v", consumer, err)
	}
}

func TestContractRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "orders.eu", "orders eu", "订单"} {
		if err := ValidateQueueName(name); err == nil {
			t.Errorf("ValidateQueueName(%q) accepted invalid name", name)
		}
	}
	for _, priority := range []int{-1, 256} {
		if err := ValidatePriority(priority); err == nil {
			t.Errorf("ValidatePriority(%d) accepted invalid priority", priority)
		}
	}
}
