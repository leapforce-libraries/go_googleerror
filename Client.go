package google

import (
	"net/http"

	errortools "github.com/leapforce-libraries/go_errortools"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
)

/*
type API string

const(
	API
)*/

// Client stores googleClient configuration
//
type GoogleClient struct {
	oAuth2   *oauth2.OAuth2
	bigQuery *BigQuery
}

type GoogleClientConfig struct {
	APIName      string
	ClientID     string
	ClientSecret string
	Scope        string
}

const (
	authURL           string = "https://accounts.google.com/o/oauth2/v2/auth"
	tokenURL          string = "https://oauth2.googleapis.com/token"
	tokenHTTPMethod   string = http.MethodPost
	redirectURL       string = "http://localhost:8080/oauth/redirect"
	tableRefreshToken string = "leapforce.oauth2"
)

// methods
//
func NewGoogleClient(googleClientConfig GoogleClientConfig, bigQuery *BigQuery) *GoogleClient {
	getTokenFunction := func() (*oauth2.Token, *errortools.Error) {
		return GetToken(googleClientConfig.APIName, googleClientConfig.ClientID, bigQuery)
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return SaveToken(googleClientConfig.APIName, googleClientConfig.ClientID, token, bigQuery)
	}

	maxRetries := uint(3)
	config := oauth2.OAuth2Config{
		ClientID:          googleClientConfig.ClientID,
		ClientSecret:      googleClientConfig.ClientSecret,
		Scope:             googleClientConfig.Scope,
		RedirectURL:       redirectURL,
		AuthURL:           authURL,
		TokenURL:          tokenURL,
		TokenHTTPMethod:   tokenHTTPMethod,
		GetTokenFunction:  &getTokenFunction,
		SaveTokenFunction: &saveTokenFunction,
		MaxRetries:        &maxRetries,
	}
	return &GoogleClient{oauth2.NewOAuth(config), bigQuery}
}

func (gc *GoogleClient) InitToken() *errortools.Error {
	return gc.oAuth2.InitToken()
}

func (gc *GoogleClient) Get(url string, headers *http.Header, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Get(url, headers, responseModel, &err)
	return request, response, gc.captureError(e, &err)
}

func (gc *GoogleClient) Post(url string, headers *http.Header, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Post(url, headers, bodyModel, responseModel, &err)
	return request, response, gc.captureError(e, &err)
}

func (gc *GoogleClient) Put(url string, headers *http.Header, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Put(url, headers, bodyModel, responseModel, &err)
	return request, response, gc.captureError(e, &err)
}

func (gc *GoogleClient) Patch(url string, headers *http.Header, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Patch(url, headers, bodyModel, responseModel, &err)
	return request, response, gc.captureError(e, &err)
}

func (gc *GoogleClient) Delete(url string, headers *http.Header, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Delete(url, headers, bodyModel, responseModel, &err)
	return request, response, gc.captureError(e, &err)
}

func (gc *GoogleClient) captureError(e *errortools.Error, err *ErrorResponse) *errortools.Error {
	if e == nil || err == nil {
		return nil
	}

	if err.Error.Message != "" {
		e.SetMessage(err.Error.Message)
	}

	return e
}
