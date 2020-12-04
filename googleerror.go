package googlesearchconsole

// GoogleError stores general google API error response
//
type GoogleError struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Errors  []struct {
			Domain  string `json:"domain"`
			Message string `json:"message"`
			Reason  string `json:"reason"`
		} `json:"errors"`
	} `json:"error"`
}
