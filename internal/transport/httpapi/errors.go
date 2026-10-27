package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/nikrybak/taskq/internal/domain"
)

// writeError is the single place where domain errors become HTTP responses.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var (
		ve       *domain.ValidationError
		tooLarge *http.MaxBytesError
	)
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, errorBody{errorDetail{Code: "validation_failed", Message: ve.Error(), Field: ve.Field}})
	case errors.As(err, &tooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody{errorDetail{Code: "body_too_large", Message: "request body is too large"}})
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorBody{errorDetail{Code: "not_found", Message: "job not found"}})
	case errors.Is(err, domain.ErrInvalidTransition):
		writeJSON(w, http.StatusConflict, errorBody{errorDetail{Code: "invalid_transition", Message: err.Error()}})
	case errors.Is(err, domain.ErrConflict):
		writeJSON(w, http.StatusConflict, errorBody{errorDetail{Code: "conflict", Message: "job changed concurrently, retry the request"}})
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		writeJSON(w, http.StatusConflict, errorBody{errorDetail{Code: "idempotency_mismatch", Message: err.Error()}})
	default:
		// Internal details stay in the log; clients get a generic message.
		log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeJSON(w, http.StatusInternalServerError, errorBody{errorDetail{Code: "internal", Message: "internal server error"}})
	}
}
