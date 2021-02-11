package gohalt

// Generator defines func signature that is able
// to generate new throttlers by provided key.
type Generator func(string) (Throttler, error)
