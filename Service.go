package google

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_http "github.com/leapforce-libraries/go_http"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
	go_token "github.com/leapforce-libraries/go_oauth2/token"
	tokensource "github.com/leapforce-libraries/go_oauth2/tokensource"
)

// Service stores GoogleService configuration
//
type Service struct {
	apiName           string
	authorizationMode authorizationMode
	clientID          string
	apiKey            *string
	accessToken       *string
	httpService       *go_http.Service
	oAuth2Service     *oauth2.Service
}

const (
	authURL            string = "https://accounts.google.com/o/oauth2/v2/auth"
	tokenURL           string = "https://oauth2.googleapis.com/token"
	tokenHTTPMethod    string = http.MethodPost
	defaultRedirectURL string = "http://localhost:8080/oauth/redirect"
	tableRefreshToken  string = "leapforce.oauth2"
)

type authorizationMode string

const (
	authorizationModeOAuth2      authorizationMode = "oauth2"
	authorizationModeApiKey      authorizationMode = "apikey"
	authorizationModeAccessToken authorizationMode = "accesstoken"
)

type ServiceWithOAuth2Config struct {
	ApiName       string
	ClientID      string
	ClientSecret  string
	TokenSource   tokensource.TokenSource
	RedirectURL   *string
	RefreshMargin *time.Duration
}

func NewServiceWithOAuth2(cfg *ServiceWithOAuth2Config) (*Service, *errortools.Error) {
	if cfg == nil {
		return nil, errortools.ErrorMessage("ServiceConfig must not be a nil pointer")
	}

	if cfg.ClientID == "" {
		return nil, errortools.ErrorMessage("ClientID not provided")
	}

	redirectURL := defaultRedirectURL
	if cfg.RedirectURL != nil {
		redirectURL = *cfg.RedirectURL
	}

	oauth2ServiceConfig := oauth2.ServiceConfig{
		ClientID:        cfg.ClientID,
		ClientSecret:    cfg.ClientSecret,
		RedirectURL:     redirectURL,
		AuthURL:         authURL,
		TokenURL:        tokenURL,
		RefreshMargin:   cfg.RefreshMargin,
		TokenHTTPMethod: tokenHTTPMethod,
		TokenSource:     cfg.TokenSource,
	}
	oauth2Service, e := oauth2.NewService(&oauth2ServiceConfig)
	if e != nil {
		return nil, e
	}

	return &Service{
		apiName:           cfg.ApiName,
		authorizationMode: authorizationModeOAuth2,
		clientID:          cfg.ClientID,
		oAuth2Service:     oauth2Service,
	}, nil
}

type ServiceWithAccessTokenConfig struct {
	ApiName     string
	AccessToken string
}

func NewServiceWithAccessToken(cfg *ServiceWithAccessTokenConfig) (*Service, *errortools.Error) {
	if cfg == nil {
		return nil, errortools.ErrorMessage("ServiceConfig must not be a nil pointer")
	}

	if cfg.AccessToken == "" {
		return nil, errortools.ErrorMessage("AccessToken not provided")
	}

	httpService, e := go_http.NewService(&go_http.ServiceConfig{})
	if e != nil {
		return nil, e
	}

	return &Service{
		apiName:           cfg.ApiName,
		authorizationMode: authorizationModeAccessToken,
		accessToken:       &cfg.AccessToken,
		httpService:       httpService,
	}, nil
}

type ServiceWithApiKeyConfig struct {
	ApiName string
	ApiKey  string
}

func NewServiceWithApiKey(cfg *ServiceWithApiKeyConfig) (*Service, *errortools.Error) {
	if cfg == nil {
		return nil, errortools.ErrorMessage("ServiceConfig must not be a nil pointer")
	}

	if cfg.ApiKey == "" {
		return nil, errortools.ErrorMessage("ApiKey not provided")
	}

	httpService, e := go_http.NewService(&go_http.ServiceConfig{})
	if e != nil {
		return nil, e
	}

	return &Service{
		apiName:           cfg.ApiName,
		authorizationMode: authorizationModeApiKey,
		apiKey:            &cfg.ApiKey,
		httpService:       httpService,
	}, nil
}

func (service *Service) InitToken(scope string, accessType *string, prompt *string, state *string) *errortools.Error {
	return service.oAuth2Service.InitToken(scope, accessType, prompt, state)
}

func (service *Service) HttpRequest(requestConfig *go_http.RequestConfig) (*http.Request, *http.Response, *errortools.Error) {
	var request *http.Request
	var response *http.Response
	var e *errortools.Error

	// add error model
	errorResponse := ErrorResponse{}
	requestConfig.ErrorModel = &errorResponse

	if service.authorizationMode == authorizationModeOAuth2 {
		request, response, e = service.oAuth2Service.HTTPRequest(requestConfig)
	} else {
		if service.authorizationMode == authorizationModeApiKey {
			// add api key
			requestConfig.SetParameter("key", *service.apiKey)
		}
		if service.accessToken != nil {
			// add accesstoken to header
			header := http.Header{}
			header.Set("Authorization", fmt.Sprintf("Bearer %s", *service.accessToken))
			requestConfig.NonDefaultHeaders = &header
		}

		request, response, e = service.httpService.HTTPRequest(requestConfig)
	}

	if e != nil {
		if errorResponse.Error.Message != "" {
			e.SetMessage(errorResponse.Error.Message)
		}
	}

	if e != nil {
		return request, response, e
	}

	return request, response, nil
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

func (service *Service) ApiName() string {
	return service.apiName
}

func (service *Service) ApiKey() string {
	return clientIDShort(service.clientID)
}

func (service *Service) ApiCallCount() int64 {
	return service.oAuth2Service.ApiCallCount()
}

func (service *Service) ApiReset() {
	service.oAuth2Service.ApiReset()
}

func clientIDShort(clientID string) string {
	return strings.Split(clientID, ".")[0]
}
