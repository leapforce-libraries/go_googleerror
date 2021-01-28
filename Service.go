package google

import (
	"net/http"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
)

// Service stores GoogleService configuration
//
type Service struct {
	oAuth2 *oauth2.OAuth2
}

type ServiceConfig struct {
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
func NewService(serviceConfig ServiceConfig, bigQueryService *bigquery.Service) *Service {
	getTokenFunction := func() (*oauth2.Token, *errortools.Error) {
		return GetToken(serviceConfig.APIName, serviceConfig.ClientID, bigQueryService)
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return SaveToken(serviceConfig.APIName, serviceConfig.ClientID, token, bigQueryService)
	}

	maxRetries := uint(3)
	oauht2Config := oauth2.OAuth2Config{
		ClientID:          serviceConfig.ClientID,
		ClientSecret:      serviceConfig.ClientSecret,
		Scope:             serviceConfig.Scope,
		RedirectURL:       redirectURL,
		AuthURL:           authURL,
		TokenURL:          tokenURL,
		TokenHTTPMethod:   tokenHTTPMethod,
		GetTokenFunction:  &getTokenFunction,
		SaveTokenFunction: &saveTokenFunction,
		MaxRetries:        &maxRetries,
	}
	return &Service{oauth2.NewOAuth(oauht2Config)}
}

func (service *Service) InitToken() *errortools.Error {
	return service.oAuth2.InitToken()
}

func (service *Service) Get(requestConfig *oauth2.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Get(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Post(requestConfig *oauth2.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Post(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Put(requestConfig *oauth2.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Put(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Patch(requestConfig *oauth2.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Patch(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Delete(requestConfig *oauth2.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Delete(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) captureError(e *errortools.Error, err *ErrorResponse) *errortools.Error {
	if e == nil || err == nil {
		return nil
	}

	if err.Error.Message != "" {
		e.SetMessage(err.Error.Message)
	}

	return e
}

func (service *Service) ValidateToken() (*oauth2.Token, *errortools.Error) {
	return service.oAuth2.ValidateToken()
}
