package google

type ErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []struct {
			Type   string `json:"@type"`
			Errors []struct {
				ErrorCode map[string]string `json:"errorCode"`
				Message   string            `json:"message"`
			} `json:"errors"`
			RequestId string `json:"requestId"`
		} `json:"details"`
	} `json:"error"`
}
