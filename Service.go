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
	oAuth2          *oauth2.OAuth2
	bigQueryService *bigquery.Service
}

type ServiceConfig struct {
	APIName           string
	ClientID          string
	ClientSecret      string
	Scope             string
	NonDefaultHeaders *http.Header
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
func NewService(serviceConfig ServiceConfig, service *bigquery.Service) *Service {
	getTokenFunction := func() (*oauth2.Token, *errortools.Error) {
		return GetToken(serviceConfig.APIName, serviceConfig.ClientID, service)
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return SaveToken(serviceConfig.APIName, serviceConfig.ClientID, token, service)
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
		NonDefaultHeaders: serviceConfig.NonDefaultHeaders,
		MaxRetries:        &maxRetries,
	}
	return &Service{oauth2.NewOAuth(oauht2Config), service}
}

func (service *Service) InitToken() *errortools.Error {
	return service.oAuth2.InitToken()
}

func (service *Service) Get(url string, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Get(url, responseModel, &err)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Post(url string, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Post(url, bodyModel, responseModel, &err)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Put(url string, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Put(url, bodyModel, responseModel, &err)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Patch(url string, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Patch(url, bodyModel, responseModel, &err)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Delete(url string, bodyModel interface{}, responseModel interface{}) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	request, response, e := service.oAuth2.Delete(url, bodyModel, responseModel, &err)
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
