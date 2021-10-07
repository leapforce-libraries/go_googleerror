package google

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	errortools "github.com/leapforce-libraries/go_errortools"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	oauth2 "github.com/leapforce-libraries/go_oauth2"
)

type Token struct {
	AccessToken  bigquery.NullString
	Scope        bigquery.NullString
	TokenType    bigquery.NullString
	RefreshToken bigquery.NullString
	Expiry       bigquery.NullTimestamp
}

func GetToken(apiName string, clientID string, service *go_bigquery.Service) (*oauth2.Token, *errortools.Error) {
	sqlSelect := "TokenType, AccessToken, RefreshToken, Expiry, Scope"
	sqlWhere := fmt.Sprintf("Api = '%s' AND ClientID = '%s'", apiName, clientID)

	token := new(Token)

	tableName := tableRefreshToken
	sqlConfig := go_bigquery.SQLConfig{
		DatasetName:     "",
		TableOrViewName: &tableName,
		SQLSelect:       &sqlSelect,
		SQLWhere:        &sqlWhere,
	}

	_, e := service.GetStruct(&sqlConfig, token)
	if e != nil {
		return nil, e
	}

	expiry := go_bigquery.NullTimestampToTime(token.Expiry)

	if expiry != nil {
		//convert to UTC
		locUTC, _ := time.LoadLocation("UTC")
		expiryUTC := (*expiry).In(locUTC)
		expiry = &expiryUTC
	}

	return &oauth2.Token{
		AccessToken:  go_bigquery.NullStringToString(token.AccessToken),
		Scope:        go_bigquery.NullStringToString(token.Scope),
		TokenType:    go_bigquery.NullStringToString(token.TokenType),
		ExpiresIn:    nil,
		RefreshToken: go_bigquery.NullStringToString(token.RefreshToken),
		Expiry:       expiry,
	}, nil
}

func SaveToken(apiName string, clientID string, token *oauth2.Token, service *go_bigquery.Service) *errortools.Error {
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

	return service.Run(sql, "saving token")
}
