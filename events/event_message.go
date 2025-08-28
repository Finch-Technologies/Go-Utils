package events

type EventMessage struct {
	SessionId      string `json:"sessionId"`
	Event          Event  `json:"event"`
	DisplayTitle   string `json:"displayTitle"`
	DisplayMessage string `json:"displayMessage"`
	Data           any    `json:"data"`
}
