package google

import (
	"fmt"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	go_token "github.com/leapforce-libraries/go_oauth2/token"
)

type TokenTable struct {
	apiName         string
	clientID        string
	token           *go_token.Token
	bigQueryService *go_bigquery.Service
}

func NewTokenTable(apiName string, clientID string, bigQueryService *go_bigquery.Service) (*TokenTable, *errortools.Error) {
	if bigQueryService == nil {
		return nil, errortools.ErrorMessage("BigQueryService is a nil pointer")
	}

	return &TokenTable{
		apiName:         apiName,
		clientID:        clientID,
		bigQueryService: bigQueryService,
	}, nil
}

func (t *TokenTable) Token() *go_token.Token {
	return t.token
}

func (t *TokenTable) NewToken() (*go_token.Token, *errortools.Error) {
	return nil, nil
}

func (t *TokenTable) SetToken(token *go_token.Token, save bool) *errortools.Error {
	t.token = token

	if !save {
		return nil
	}

	return t.SaveToken()
}

func (t *TokenTable) RetrieveToken() *errortools.Error {
	sqlSelect := "TokenType, AccessToken, RefreshToken, Expiry, Scope"
	sqlWhere := fmt.Sprintf("Api = '%s' AND ClientID = '%s'", t.apiName, t.clientID)

	token := new(go_token.Token)

	tableName := tableRefreshToken
	sqlConfig := go_bigquery.SQLConfig{
		DatasetName:     "",
		TableOrViewName: &tableName,
		SQLSelect:       &sqlSelect,
		SQLWhere:        &sqlWhere,
	}

	_, e := t.bigQueryService.GetStruct(&sqlConfig, token)
	if e != nil {
		return e
	}

	//convert to UTC
	locUTC, _ := time.LoadLocation("UTC")
	expiryUTC := token.Expiry.In(locUTC)
	token.Expiry = &expiryUTC

	t.token = token

	return nil
}

func (t *TokenTable) SaveToken() *errortools.Error {
	if t.token == nil {
		return nil
	}

	sqlUpdate := "SET AccessToken = SOURCE.AccessToken, Expiry = SOURCE.Expiry"

	tokenType := "NULLIF('','')"
	if t.token.TokenType != nil {
		if *t.token.TokenType != "" {
			tokenType = fmt.Sprintf("'%s'", *t.token.TokenType)
			sqlUpdate = fmt.Sprintf("%s, TokenType = SOURCE.TokenType", sqlUpdate)
		}
	}

	accessToken := "NULLIF('','')"
	if t.token.AccessToken != nil {
		if *t.token.AccessToken != "" {
			accessToken = fmt.Sprintf("'%s'", *t.token.AccessToken)
		}
	}

	refreshToken := "NULLIF('','')"
	if t.token.RefreshToken != nil {
		if *t.token.RefreshToken != "" {
			refreshToken = fmt.Sprintf("'%s'", *t.token.RefreshToken)
			sqlUpdate = fmt.Sprintf("%s, RefreshToken = SOURCE.RefreshToken", sqlUpdate)
		}
	}

	expiry := "TIMESTAMP(NULL)"
	if t.token.Expiry != nil {
		expiry = fmt.Sprintf("TIMESTAMP('%s')", (*t.token.Expiry).Format("2006-01-02T15:04:05"))
	}

	scope := "NULLIF('','')"
	if t.token.Scope != nil {
		if *t.token.Scope != "" {
			scope = fmt.Sprintf("'%s'", *t.token.Scope)
			sqlUpdate = fmt.Sprintf("%s, Scope = SOURCE.Scope", sqlUpdate)
		}
	}

	sql := "MERGE `" + tableRefreshToken + "` AS TARGET " +
		"USING  (SELECT '" +
		t.apiName + "' AS Api,'" +
		t.clientID + "' AS ClientID," +
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

	return t.bigQueryService.Run(sql, "saving token")
}
