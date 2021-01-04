package google

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
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
		return getToken(googleClientConfig.APIName, googleClientConfig.ClientID, bigQuery)
	}

	saveTokenFunction := func(token *oauth2.Token) *errortools.Error {
		return saveToken(googleClientConfig.APIName, googleClientConfig.ClientID, token, bigQuery)
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

func getToken(apiName string, clientID string, bq *BigQuery) (*oauth2.Token, *errortools.Error) {
	sqlSelect := "TokenType, AccessToken, RefreshToken, Expiry, Scope"
	sqlWhere := fmt.Sprintf("Api = '%s' AND ClientID = '%s'", apiName, clientID)

	type TokenBQ struct {
		AccessToken  bigquery.NullString
		Scope        bigquery.NullString
		TokenType    bigquery.NullString
		RefreshToken bigquery.NullString
		Expiry       bigquery.NullTimestamp
	}

	tokenBQ := new(TokenBQ)

	e := bq.GetStruct("", tableRefreshToken, sqlSelect, sqlWhere, tokenBQ)
	if e != nil {
		return nil, e
	}

	expiry := NullTimestampToTime(tokenBQ.Expiry)

	if expiry != nil {
		//convert to UTC
		locUTC, _ := time.LoadLocation("UTC")
		expiryUTC := (*expiry).In(locUTC)
		expiry = &expiryUTC
	}

	return &oauth2.Token{
		NullStringToString(tokenBQ.AccessToken),
		NullStringToString(tokenBQ.Scope),
		NullStringToString(tokenBQ.TokenType),
		nil,
		NullStringToString(tokenBQ.RefreshToken),
		expiry,
	}, nil
}

func saveToken(apiName string, clientID string, token *oauth2.Token, bq *BigQuery) *errortools.Error {
	if token == nil {
		return nil
	}

	sqlUpdate := "SET AccessToken = SOURCE.AccessToken, Expiry = SOURCE.Expiry"

	tokenType := "NULLIF('','')"
	if token.TokenType != nil {
		if *token.TokenType != "" {
			tokenType = fmt.Sprintf("'%s'", *token.TokenType)
			sqlUpdate = fmt.Sprintf("%s, TokenType = SOURCE.TokenType", sqlUpdate)
		}
	}

	accessToken := "NULLIF('','')"
	if token.AccessToken != nil {
		if *token.AccessToken != "" {
			accessToken = fmt.Sprintf("'%s'", *token.AccessToken)
		}
	}

	refreshToken := "NULLIF('','')"
	if token.RefreshToken != nil {
		if *token.RefreshToken != "" {
			refreshToken = fmt.Sprintf("'%s'", *token.RefreshToken)
			sqlUpdate = fmt.Sprintf("%s, RefreshToken = SOURCE.RefreshToken", sqlUpdate)
		}
	}

	expiry := "TIMESTAMP(NULL)"
	if token.Expiry != nil {
		expiry = fmt.Sprintf("TIMESTAMP('%s')", (*token.Expiry).Format("2006-01-02T15:04:05"))
	}

	scope := "NULLIF('','')"
	if token.Scope != nil {
		if *token.Scope != "" {
			scope = fmt.Sprintf("'%s'", *token.Scope)
			sqlUpdate = fmt.Sprintf("%s, Scope = SOURCE.Scope", sqlUpdate)
		}
	}

	sql := "MERGE `" + tableRefreshToken + "` AS TARGET " +
		"USING  (SELECT '" +
		apiName + "' AS Api,'" +
		clientID + "' AS ClientID," +
		tokenType + " AS TokenType," +
		accessToken + " AS AccessToken," +
		refreshToken + " AS RefreshToken," +
		expiry + " AS Expiry," +
		scope + " AS Scope) AS SOURCE " +
		" ON TARGET.Api = SOURCE.Api " +
		" AND TARGET.ClientID = SOURCE.ClientID " +
		"WHEN MATCHED THEN " +
		"	UPDATE " + sqlUpdate +
		" WHEN NOT MATCHED BY TARGET THEN " +
		"	INSERT (Api, ClientID, TokenType, AccessToken, RefreshToken, Expiry, Scope) " +
		"	VALUES (SOURCE.Api, SOURCE.ClientID, SOURCE.TokenType, SOURCE.AccessToken, SOURCE.RefreshToken, SOURCE.Expiry, SOURCE.Scope)"

	return bq.Run(nil, sql, "saving token")
}
