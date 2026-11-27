// Package signing signs webhook requests per Standard Webhooks
// (https://www.standardwebhooks.com/): webhook-id, webhook-timestamp and
// webhook-signature.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Prefix marks a Standard Webhooks secret.
const Prefix = "whsec_"

// Header names.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// Key decodes a secret of the form "whsec_" + base64.
func Key(secret string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, Prefix))
	if err != nil || len(key) == 0 {
		return nil, errors.New("webhook secret must be base64, prefixed with " + Prefix)
	}
	return key, nil
}

// Signature returns "v1," + base64(HMAC-SHA256(key, id.timestamp.body)).
func Signature(key []byte, id string, timestamp int64, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id))
	mac.Write([]byte{'.'})
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Sign sets the three headers on h. Each of more adds a signature, space-separated
// after the first: during a secret rotation the request is signed with the new key
// and the old one, and a receiver holding either accepts it.
func Sign(h http.Header, key []byte, id string, at time.Time, body []byte, more ...[]byte) {
	ts := at.Unix()
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	sigs := []string{Signature(key, id, ts, body)}
	for _, k := range more {
		sigs = append(sigs, Signature(k, id, ts, body))
	}
	h.Set(HeaderSignature, strings.Join(sigs, " "))
}

// Verify checks the headers of a received request the way a receiver does: the
// timestamp within tolerance of now, and one of the space-separated signatures valid.
func Verify(h http.Header, key []byte, body []byte, now time.Time, tolerance time.Duration) error {
	id := h.Get(HeaderID)
	ts, err := strconv.ParseInt(h.Get(HeaderTimestamp), 10, 64)
	if id == "" || err != nil {
		return errors.New("missing webhook-id or webhook-timestamp")
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return errors.New("timestamp outside tolerance")
	}
	want := Signature(key, id, ts, body)
	for _, sig := range strings.Fields(h.Get(HeaderSignature)) {
		if hmac.Equal([]byte(sig), []byte(want)) {
			return nil
		}
	}
	return errors.New("no valid signature")
}
