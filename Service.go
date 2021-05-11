package google

import (
	"net/http"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	go_http "github.com/leapforce-libraries/go_http"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
)

// Service stores GoogleService configuration
//
type Service struct {
	apiName       string
	clientID      string
	oAuth2Service *oauth2.Service
}

type ServiceConfig struct {
	APIName      string
	ClientID     string
	ClientSecret string
	RedirectURL  *string
}

const (
	authURL            string = "https://accounts.google.com/o/oauth2/v2/auth"
	tokenURL           string = "https://oauth2.googleapis.com/token"
	tokenHTTPMethod    string = http.MethodPost
	defaultRedirectURL string = "http://localhost:8080/oauth/redirect"
	tableRefreshToken  string = "leapforce.oauth2"
)

// methods
//
func NewService(serviceConfig *ServiceConfig, bigQueryService *bigquery.Service) (*Service, *errortools.Error) {
	if serviceConfig == nil {
		return nil, errortools.ErrorMessage("ServiceConfig must not be a nil pointer")
	}

	getTokenFunction := func() (*oauth2.Token, *errortools.Error) {
		return GetToken(serviceConfig.APIName, serviceConfig.ClientID, bigQueryService)
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return SaveToken(serviceConfig.APIName, serviceConfig.ClientID, token, bigQueryService)
	}

	redirectURL := defaultRedirectURL
	if serviceConfig.RedirectURL != nil {
		redirectURL = *serviceConfig.RedirectURL
	}

	oauth2ServiceConfig := oauth2.ServiceConfig{
		ClientID:          serviceConfig.ClientID,
		ClientSecret:      serviceConfig.ClientSecret,
		RedirectURL:       redirectURL,
		AuthURL:           authURL,
		TokenURL:          tokenURL,
		TokenHTTPMethod:   tokenHTTPMethod,
		GetTokenFunction:  &getTokenFunction,
		SaveTokenFunction: &saveTokenFunction,
	}
	oauth2Service, e := oauth2.NewService(&oauth2ServiceConfig)
	if e != nil {
		return nil, e
	}

	return &Service{
		apiName:       serviceConfig.APIName,
		clientID:      serviceConfig.ClientID,
		oAuth2Service: oauth2Service,
	}, nil
}

func (service *Service) InitToken(scope string) *errortools.Error {
	return service.oAuth2Service.InitToken(scope)
}

func (service *Service) Get(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err
	request, response, e := service.oAuth2Service.Get(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Post(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err
	request, response, e := service.oAuth2Service.Post(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Put(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err
	request, response, e := service.oAuth2Service.Put(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Patch(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err
	request, response, e := service.oAuth2Service.Patch(requestConfig)
	return request, response, service.captureError(e, &err)
}

func (service *Service) Delete(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err
	request, response, e := service.oAuth2Service.Delete(requestConfig)
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
	return service.oAuth2Service.ValidateToken()
}

func (service *Service) AuthorizeURL(scope string) string {
	return service.oAuth2Service.AuthorizeURL(scope)
}

func (service *Service) GetAccessTokenFromCode(r *http.Request) *errortools.Error {
	return service.oAuth2Service.GetAccessTokenFromCode(r)
}

func (service *Service) APIName() string {
	return service.apiName
}

func (service *Service) APIKey() string {
	return service.clientID
}

func (service *Service) APICallCount() int64 {
	return service.oAuth2Service.APICallCount()
}

func (service *Service) APIReset() {
	service.oAuth2Service.APIReset()
}
