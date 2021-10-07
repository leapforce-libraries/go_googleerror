package google

import (
	"net/http"
	"strings"

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
	APIName           string
	ClientID          string
	ClientSecret      string
	RedirectURL       *string
	GetTokenFunction  *func() (*oauth2.Token, *errortools.Error)
	SaveTokenFunction *func(token *oauth2.Token) *errortools.Error
}

const (
	authURL            string = "https://accounts.google.com/o/oauth2/v2/auth"
	tokenURL           string = "https://oauth2.googleapis.com/token"
	tokenHTTPMethod    string = http.MethodPost
	defaultRedirectURL string = "http://localhost:8080/oauth/redirect"
	tableRefreshToken  string = "leapforce.oauth2"
)

type AuthorizationMode string

const (
	AuthorizationModeOAuth2      AuthorizationMode = "oauth2"
	AuthorizationModeAPIKey      AuthorizationMode = "apikey"
	AuthorizationModeAccessToken AuthorizationMode = "accesstoken"
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
	if serviceConfig.GetTokenFunction != nil {
		getTokenFunction = *serviceConfig.GetTokenFunction
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return SaveToken(serviceConfig.APIName, serviceConfig.ClientID, token, bigQueryService)
	}
	if serviceConfig.SaveTokenFunction != nil {
		saveTokenFunction = *serviceConfig.SaveTokenFunction
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

func (service *Service) InitToken(scope string, accessType *string, prompt *string, state *string) *errortools.Error {
	return service.oAuth2Service.InitToken(scope, accessType, prompt, state)
}

func (service *Service) GetToken() *oauth2.Token {
	return service.oAuth2Service.GetToken()
}

func (service *Service) SetToken(token *oauth2.Token) {
	service.oAuth2Service.SetToken(token)
}

func (service *Service) HTTPRequest(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err

	request, response, e := service.oAuth2Service.HTTPRequest(requestConfig.Method, requestConfig)

	if e != nil {
		if err.Error.Message != "" {
			e.SetMessage(err.Error.Message)
		}
	}

	return request, response, e
}

func (service *Service) ValidateToken() (*oauth2.Token, *errortools.Error) {
	return service.oAuth2Service.ValidateToken()
}

func (service *Service) AuthorizeURL(scope string, accessType *string, prompt *string, state *string) string {
	return service.oAuth2Service.AuthorizeURL(scope, accessType, prompt, state)
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

func ClientIDShort(clientID string) string {
	return strings.Split(clientID, ".")[0]
}
