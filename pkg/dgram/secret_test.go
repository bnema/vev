package dgram

import "testing"

func TestSecretDoRunsSynchronously(t *testing.T) {
	called := false

	SecretDo(func() {
		called = true
	})

	if !called {
		t.Fatal("SecretDo returned before callback ran")
	}
}

func TestSecretDoPropagatesPanic(t *testing.T) {
	want := &struct{ message string }{message: "secret callback panic"}
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("recover() = %#v, want %#v", got, want)
		}
	}()

	SecretDo(func() {
		panic(want)
	})
}
