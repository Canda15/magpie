package update

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// A release's files are trusted by one signature, not by the feed's word.
// The feed (usemagpie.ai/api/latest) is only a courier: whoever can change
// its answer, the site, a proxy, a mirror, could swap a file and the hash
// beside it. Release CI signs "magpie release <version>\n" followed by the
// release's SHA256SUMS with a private key that lives only as the
// UPDATE_SIGNING_KEY secret of yetone/magpie-releases; the feed carries the
// SHA256SUMS text and the signature (Release.Signed), and magpie checks it
// against signingKey, below, which is built into the binary. A file's hash is
// then the one the signed list gives, and a feed hash that differs is refused.
//
// Upstream requires a signature from 0.1.3862 on, and a build that holds
// its key is a release from then on and only moves to a newer one. The fork
// turns that requirement off (see signedRequired): the key lives only in
// yetone/magpie-releases, which a fork cannot sign with, so every fork build
// at or after 0.1.3862 would otherwise be refused forever. A release that
// does carry a signature is still checked.

// signingKey is the public half (base64, raw 32 bytes) of the release
// signing key. A var for tests.
var signingKey = "3XuYQEohIRDF+zxGQry1iXGZ1cfKYFX3vsST6GSSbYQ="

// Signed is a release's SHA256SUMS as published and its ed25519 signature
// (base64).
type Signed struct {
	Sums string `json:"sums"`
	Sig  string `json:"sig"`
}

// signedRequired reports whether version must carry a valid signature.
//
// 本 fork 关闭这道要求：发布签名用的私钥只存在于上游 yetone/magpie-releases，
// 我们的 xmod 工作流拿不到它，因此任何与上游同版本号(>= 0.1.3862)的 fork
// 构建都永远过不了签名校验，也就永远收不到更新（本机 0.1.4037+xmod.* 卡在
// 0.1.3862 之后即是此因）。fork 的 feed 由自己仓库的 HTTPS 发布，latest.json
// 里给出每个文件的 SHA-256，下载时仍按该哈希校验字节；若某条 release 恰好带了
// 签名，verify 依旧会验证它，只是不再强制。attestation（attest.go）只在签名
// 存在时才会置 a.attest，故这里放开后它也随之不再生效。
func signedRequired(string) bool { return false }

// verify checks rel against the embedded key and, when it is signed, sets
// each asset's SHA256 to the signed list's and drops the files that list
// doesn't name. A release that must be signed and isn't, whose signature
// fails, or whose feed hash differs from the signed one is an error.
func (rel *Release) verify() error {
	if rel.Signed == nil {
		if signedRequired(rel.Version) {
			return fmt.Errorf("release %s is not signed; not installed", rel.Version)
		}
		return nil
	}
	pub, err := base64.StdEncoding.DecodeString(signingKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return errors.New("update signing key is not valid")
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rel.Signed.Sig))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("release %s: the signature is not valid; not installed", rel.Version)
	}
	msg := "magpie release " + rel.Version + "\n" + rel.Signed.Sums
	if !ed25519.Verify(pub, []byte(msg), sig) {
		return fmt.Errorf("release %s: the signature does not match; not installed", rel.Version)
	}
	sums := map[string]string{}
	for _, line := range strings.Split(rel.Signed.Sums, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		if b, err := hex.DecodeString(f[0]); err != nil || len(b) != 32 {
			continue
		}
		sums[strings.TrimPrefix(f[1], "*")] = strings.ToLower(f[0])
	}
	for name, a := range rel.Assets {
		want, ok := sums[name]
		if !ok {
			delete(rel.Assets, name)
			continue
		}
		if a.SHA256 != "" && !strings.EqualFold(a.SHA256, want) {
			return fmt.Errorf("release %s: the feed's checksum for %s is not the signed one; not installed", rel.Version, name)
		}
		a.SHA256 = want
		a.attest = !Newer(firstAttested, rel.Version)
		rel.Assets[name] = a
	}
	return nil
}
