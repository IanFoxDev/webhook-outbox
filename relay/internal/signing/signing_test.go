package signing

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// The vector of psp-sandbox, which signs the same way.
const (
	vectorSecret    = "whsec_dGVzdC1zZWNyZXQ="
	vectorID        = "evt_1"
	vectorTimestamp = 1790330400
	vectorBody      = `{"id":"evt_1","type":"payment.captured"}`
	vectorSignature = "v1,DpPc9pmsF6HhjneOVQvZ7vDaVsOWIJZaIjvPPCJpZs8="
)

func TestVector(t *testing.T) {
	key, err := Key(vectorSecret)
	if err != nil {
		t.Fatal(err)
	}
	if got := Signature(key, vectorID, vectorTimestamp, []byte(vectorBody)); got != vectorSignature {
		t.Fatalf("signature = %s, want %s", got, vectorSignature)
	}
}

func TestSignAndVerify(t *testing.T) {
	key, _ := Key(vectorSecret)
	at := time.Unix(vectorTimestamp, 0)
	h := http.Header{}
	Sign(h, key, vectorID, at, []byte(vectorBody))
	if h.Get("Webhook-Signature") != vectorSignature || h.Get("Webhook-Timestamp") != "1790330400" {
		t.Fatalf("headers = %v", h)
	}
	if err := Verify(h, key, []byte(vectorBody), at.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if Verify(h, key, []byte(vectorBody+" "), at, time.Minute) == nil {
		t.Fatal("changed body verified")
	}
	if Verify(h, key, []byte(vectorBody), at.Add(10*time.Minute), 5*time.Minute) == nil {
		t.Fatal("old timestamp verified")
	}
	other, _ := Key("whsec_b3RoZXI=")
	if Verify(h, other, []byte(vectorBody), at, time.Minute) == nil {
		t.Fatal("other key verified")
	}
}

func TestSignWithTwoKeys(t *testing.T) {
	key, _ := Key(vectorSecret)
	old, _ := Key("whsec_b2xkLXNlY3JldA==")
	at := time.Unix(vectorTimestamp, 0)
	h := http.Header{}
	Sign(h, key, vectorID, at, []byte(vectorBody), old)
	if sigs := strings.Fields(h.Get("Webhook-Signature")); len(sigs) != 2 || sigs[0] != vectorSignature {
		t.Fatalf("signature header %q", h.Get("Webhook-Signature"))
	}
	for _, k := range [][]byte{key, old} {
		if err := Verify(h, k, []byte(vectorBody), at, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInvalidSecret(t *testing.T) {
	for _, secret := range []string{"", "whsec_", "whsec_not base64!"} {
		if _, err := Key(secret); err == nil {
			t.Errorf("Key(%q) accepted", secret)
		}
	}
}
