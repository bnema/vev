package protocol

import "testing"

func TestUIReceiptValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		receipt UIReceipt
		valid   bool
	}{
		{"processed", UIReceipt{ActionID: 1, Epoch: 2, State: 3, ViewPublication: 4, Outcome: UIReceiptProcessed}, true},
		{"unavailable", UIReceipt{ActionID: 1, Outcome: UIReceiptUnavailable}, true},
		{"zero action", UIReceipt{Outcome: UIReceiptUnavailable}, false},
		{"zero epoch", UIReceipt{ActionID: 1, State: 3, ViewPublication: 4, Outcome: UIReceiptProcessed}, false},
		{"zero state", UIReceipt{ActionID: 1, Epoch: 2, ViewPublication: 4, Outcome: UIReceiptProcessed}, false},
		{"zero publication", UIReceipt{ActionID: 1, Epoch: 2, State: 3, Outcome: UIReceiptProcessed}, false},
		{"unknown outcome", UIReceipt{ActionID: 1, Epoch: 2, State: 3, ViewPublication: 4, Outcome: 3}, false},
		{"unavailable invents boundary", UIReceipt{ActionID: 1, Epoch: 2, State: 3, ViewPublication: 4, Outcome: UIReceiptUnavailable}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.receipt.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, test.valid)
			}
		})
	}
}
