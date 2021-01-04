package google

import (
	"bytes"
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

func (gc *GoogleClient) Get(url string, model interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Get(url, model, &err)
	if e != nil {
		if err.Error.Message != "" {
			e.SetMessage(err.Error.Message)
		}

		return request, response, e
	}

	return request, response, nil
}

func (gc *GoogleClient) Patch(url string, requestBody []byte, model interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := gc.oAuth2.Patch(url, bytes.NewBuffer(requestBody), model, &err)
	if e != nil {
		if err.Error.Message != "" {
			e.SetMessage(err.Error.Message)
		}

		return request, response, e
	}

	return request, response, nil
}
