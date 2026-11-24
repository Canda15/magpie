package update

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// signedRelease is a release of version v whose files are signed by a key
// made for the test, which becomes the embedded one.
func signedRelease(t *testing.T, v string, files map[string]string) *Release {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(nil)
	old := signingKey
	signingKey = base64.StdEncoding.EncodeToString(pub)
	t.Cleanup(func() { signingKey = old })
	t.Setenv("MAGPIE_UPDATE_FEED", "")
	var sums strings.Builder
	rel := &Release{Version: v, Assets: map[string]Asset{}}
	for name, body := range files {
		h := sha256.Sum256([]byte(body))
		sums.WriteString(hex.EncodeToString(h[:]) + "  " + name + "\n")
		rel.Assets[name] = Asset{URL: "https://x/" + name, SHA256: hex.EncodeToString(h[:])}
	}
	sig := ed25519.Sign(priv, []byte("magpie release "+v+"\n"+sums.String()))
	rel.Signed = &Signed{Sums: sums.String(), Sig: base64.StdEncoding.EncodeToString(sig)}
	return rel
}

func TestSignedReleaseAccepted(t *testing.T) {
	rel := signedRelease(t, "0.1.4000", map[string]string{"magpie-linux-amd64": "bin", "magpie-darwin-arm64.zip": "zip"})
	if err := rel.verify(); err != nil {
		t.Fatal(err)
	}
	if len(rel.Assets) != 2 {
		t.Fatalf("assets %v", rel.Assets)
	}
}

// A hash the feed gives for itself is replaced by the signed one, and a
// file the signed list doesn't name is dropped.
func TestSignedListGivesTheHash(t *testing.T) {
	rel := signedRelease(t, "0.1.4000", map[string]string{"a": "1"})
	a := rel.Assets["a"]
	a.SHA256 = ""
	rel.Assets["a"] = a
	rel.Assets["extra"] = Asset{URL: "https://evil/extra", SHA256: "00"}
	if err := rel.verify(); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256([]byte("1"))
	if rel.Assets["a"].SHA256 != hex.EncodeToString(h[:]) {
		t.Fatalf("hash %q", rel.Assets["a"].SHA256)
	}
	if _, ok := rel.Assets["extra"]; ok {
		t.Fatal("an unsigned file stayed")
	}
}

func TestTamperedReleaseRefused(t *testing.T) {
	cases := map[string]func(*Release){
		"feed hash swapped": func(r *Release) {
			a := r.Assets["a"]
			a.SHA256 = strings.Repeat("ab", 32)
			r.Assets["a"] = a
		},
		"signed list swapped": func(r *Release) {
			r.Signed.Sums = strings.Repeat("ab", 32) + "  a\n"
		},
		"signature flipped": func(r *Release) {
			b, _ := base64.StdEncoding.DecodeString(r.Signed.Sig)
			b[0] ^= 1
			r.Signed.Sig = base64.StdEncoding.EncodeToString(b)
		},
		"signature garbage": func(r *Release) { r.Signed.Sig = "zz" },
		"other version":     func(r *Release) { r.Version = "0.1.4001" },
	}
	for name, tamper := range cases {
		t.Run(name, func(t *testing.T) {
			rel := signedRelease(t, "0.1.4000", map[string]string{"a": "1"})
			tamper(rel)
			if err := rel.verify(); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// Another key's signature is no signature.
func TestSignedByOtherKeyRefused(t *testing.T) {
	rel := signedRelease(t, "0.1.4000", map[string]string{"a": "1"})
	pub, _, _ := ed25519.GenerateKey(nil)
	signingKey = base64.StdEncoding.EncodeToString(pub)
	if rel.verify() == nil {
		t.Fatal("accepted")
	}
}

// The fork turns the signature requirement off (signedRequired), because its
// CI cannot hold the upstream key: an unsigned release — every xmod build —
// is accepted with the feed's own SHA-256, while a release that does carry a
// signature not made by the embedded key is still refused.
func TestUnsignedReleaseAccepted(t *testing.T) {
	t.Setenv("MAGPIE_UPDATE_FEED", "")
	old := &Release{Version: "0.1.3000", Assets: map[string]Asset{"a": {SHA256: "ab"}}}
	if err := old.verify(); err != nil {
		t.Fatal(err)
	}
	if old.Assets["a"].SHA256 != "ab" {
		t.Fatal("hash changed")
	}
	// 0.1.4757 是 xmod feed 真实的版本号，无 signed 块，必须放行。
	cur := &Release{Version: "0.1.4757", Assets: map[string]Asset{"a": {SHA256: "ab"}}}
	if err := cur.verify(); err != nil {
		t.Fatalf("an unsigned fork release was refused: %v", err)
	}
	bad := signedRelease(t, "0.1.4757", map[string]string{"a": "1"})
	bad.Signed.Sig = base64.StdEncoding.EncodeToString(make([]byte, 64))
	if bad.verify() == nil {
		t.Fatal("a signature not made by the embedded key was accepted")
	}
}

// The embedded key is the one release CI signs with: a sample signed by
// the private key (UPDATE_SIGNING_KEY) with openssl, as the workflow does.
func TestEmbeddedKeyVerifiesCIsSignature(t *testing.T) {
	pub, _ := base64.StdEncoding.DecodeString(signingKey)
	msg, err := os.ReadFile("testdata/signed-sample.msg")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile("testdata/signed-sample.sig")
	sig, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if !ed25519.Verify(pub, msg, sig) {
		t.Fatal("the embedded key does not verify CI's signature")
	}
}

// The feed path end to end: a tampered answer is an error, a signed one a release.
func TestLatestVerifiesTheFeed(t *testing.T) {
	rel := signedRelease(t, "0.1.4000", map[string]string{"a": "1"})
	serve := func(r *Release) string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(r) }))
		t.Cleanup(s.Close)
		return s.URL
	}
	if _, err := latestAt(context.Background(), serve(rel), ""); err != nil {
		t.Fatal(err)
	}
	bad := *rel
	bad.Assets = map[string]Asset{"a": {URL: "https://evil/a", SHA256: strings.Repeat("cd", 32)}}
	if _, err := latestAt(context.Background(), serve(&bad), ""); err == nil {
		t.Fatal("a swapped hash was taken")
	}
}
