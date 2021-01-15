package google

import (
	"fmt"
	"time"

	_bigquery "cloud.google.com/go/bigquery"
	errortools "github.com/leapforce-libraries/go_errortools"
	bigquery "github.com/leapforce-libraries/go_google/bigquery"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
)

type Token struct {
	AccessToken  _bigquery.NullString
	Scope        _bigquery.NullString
	TokenType    _bigquery.NullString
	RefreshToken _bigquery.NullString
	Expiry       _bigquery.NullTimestamp
}

func GetToken(apiName string, clientID string, service *bigquery.Service) (*oauth2.Token, *errortools.Error) {
	sqlSelect := "TokenType, AccessToken, RefreshToken, Expiry, Scope"
	sqlWhere := fmt.Sprintf("Api = '%s' AND ClientID = '%s'", apiName, clientID)

	token := new(Token)

	e := service.GetStruct("", tableRefreshToken, sqlSelect, sqlWhere, token)
	if e != nil {
		return nil, e
	}

	expiry := bigquery.NullTimestampToTime(token.Expiry)

	if expiry != nil {
		//convert to UTC
		locUTC, _ := time.LoadLocation("UTC")
		expiryUTC := (*expiry).In(locUTC)
		expiry = &expiryUTC
	}

	return &oauth2.Token{
		bigquery.NullStringToString(token.AccessToken),
		bigquery.NullStringToString(token.Scope),
		bigquery.NullStringToString(token.TokenType),
		nil,
		bigquery.NullStringToString(token.RefreshToken),
		expiry,
	}, nil
}

func SaveToken(apiName string, clientID string, token *oauth2.Token, service *bigquery.Service) *errortools.Error {
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

	return service.Run(nil, sql, "saving token")
}
