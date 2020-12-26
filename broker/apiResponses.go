package broker

type ErrorRes struct {
	Ok    bool
	Error error
}

type Res struct {
	Ok   bool
	Data interface{}
}

// ResSubscriptions is used by SUBS COMMAND
type ResSubscriptions struct {
	Ok   bool
	Data []string
}

type ResMessages struct {
	Ok   bool
	Data []Message
}
