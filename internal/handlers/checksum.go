package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nikrybak/taskq/internal/domain"
)

// ChecksumType is the job type served by Checksum.
const ChecksumType = "checksum"

// MaxChecksumRounds caps the work a single job may request.
const MaxChecksumRounds = 50_000_000

// ChecksumPayload is the payload of a checksum job.
type ChecksumPayload struct {
	Data   string `json:"data"`
	Rounds int    `json:"rounds"`
}

// Checksum is a CPU-bound demo handler: it computes an iterated SHA-256 of
// the input, standing in for work such as image resizing or report
// rendering.
type Checksum struct {
	Log *slog.Logger
}

// Handle implements worker.Handler.
func (h Checksum) Handle(ctx context.Context, job domain.Job) error {
	var p ChecksumPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return domain.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	if p.Rounds < 1 || p.Rounds > MaxChecksumRounds {
		return domain.Permanent(fmt.Errorf("rounds must be between 1 and %d", MaxChecksumRounds))
	}
	sum, err := Stretch(ctx, []byte(p.Data), p.Rounds)
	if err != nil {
		return err
	}
	h.Log.InfoContext(ctx, "checksum computed", "job_id", job.ID, "rounds", p.Rounds, "sum", hex.EncodeToString(sum[:]))
	return nil
}

// cancelCheckEvery trades responsiveness against overhead: checking the
// context costs far more than a hash round, so do it periodically.
const cancelCheckEvery = 4096

// Stretch hashes data, then re-hashes the digest rounds-1 more times. CPU
// loops never block on a channel, so they must poll ctx themselves to honour
// cancellation.
func Stretch(ctx context.Context, data []byte, rounds int) ([sha256.Size]byte, error) {
	if rounds < 1 {
		return [sha256.Size]byte{}, errors.New("rounds must be positive")
	}
	sum := sha256.Sum256(data)
	for i := 1; i < rounds; i++ {
		if i%cancelCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return [sha256.Size]byte{}, fmt.Errorf("checksum interrupted after %d rounds: %w", i, err)
			}
		}
		sum = sha256.Sum256(sum[:])
	}
	return sum, nil
}
