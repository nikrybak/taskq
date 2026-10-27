package domain

import "fmt"

// State is the lifecycle state of a job.
type State string

// Job states. The same set is enforced by a CHECK constraint in the database.
const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// AllStates lists every state in a stable order.
var AllStates = []State{StateQueued, StateRunning, StateSucceeded, StateDead, StateCancelled}

// transitions is the complete state machine. Anything not listed is illegal.
var transitions = map[State][]State{
	// claimed by a worker, or cancelled before it ran
	StateQueued: {StateRunning, StateCancelled},
	// finished, rescheduled for another attempt (failure or lost lease), or buried
	StateRunning:   {StateSucceeded, StateQueued, StateDead},
	StateDead:      {StateQueued},
	StateCancelled: {StateQueued},
	StateSucceeded: nil,
}

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether no worker will touch a job in this state again
// without an explicit operator action.
func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateDead || s == StateCancelled
}

// CanTransition reports whether moving from one state to another is allowed.
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// CheckTransition returns a *TransitionError if the move is not allowed.
func CheckTransition(from, to State) error {
	if !CanTransition(from, to) {
		return &TransitionError{From: from, To: to}
	}
	return nil
}

// ParseState converts user input into a State.
func ParseState(s string) (State, error) {
	st := State(s)
	if !st.Valid() {
		return "", &ValidationError{Field: "state", Reason: fmt.Sprintf("unknown state %q", s)}
	}
	return st, nil
}
