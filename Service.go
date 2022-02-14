package google

import (
	"net/http"
	"strings"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	go_http "github.com/leapforce-libraries/go_http"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
	go_token "github.com/leapforce-libraries/go_oauth2/token"
	tokensource "github.com/leapforce-libraries/go_oauth2/tokensource"
)

// Service stores GoogleService configuration
//
type Service struct {
	apiName       string
	clientID      string
	oAuth2Service *oauth2.Service
}

type ServiceConfig struct {
	APIName       string
	ClientID      string
	ClientSecret  string
	TokenSource   tokensource.TokenSource
	RedirectURL   *string
	RefreshMargin *time.Duration
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

	redirectURL := defaultRedirectURL
	if serviceConfig.RedirectURL != nil {
		redirectURL = *serviceConfig.RedirectURL
	}

	oauth2ServiceConfig := oauth2.ServiceConfig{
		ClientID:        serviceConfig.ClientID,
		ClientSecret:    serviceConfig.ClientSecret,
		RedirectURL:     redirectURL,
		AuthURL:         authURL,
		TokenURL:        tokenURL,
		RefreshMargin:   serviceConfig.RefreshMargin,
		TokenHTTPMethod: tokenHTTPMethod,
		TokenSource:     serviceConfig.TokenSource,
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

func (service *Service) HTTPRequest(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	err := ErrorResponse{}
	requestConfig.ErrorModel = &err

	request, response, e := service.oAuth2Service.HTTPRequest(requestConfig)

	if e != nil {
		if err.Error.Message != "" {
			e.SetMessage(err.Error.Message)
		}
	}

	return request, response, e
}

func (service *Service) AuthorizeURL(scope string, accessType *string, prompt *string, state *string) string {
	return service.oAuth2Service.AuthorizeURL(scope, accessType, prompt, state)
}

func (service *Service) ValidateToken() (*go_token.Token, *errortools.Error) {
	return service.oAuth2Service.ValidateToken()
}

func (service *Service) GetTokenFromCode(r *http.Request) *errortools.Error {
	return service.oAuth2Service.GetTokenFromCode(r)
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
