package google

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	errortools "github.com/leapforce-libraries/go_errortools"
	credentials "github.com/leapforce-libraries/go_google/credentials"
	types "github.com/leapforce-libraries/go_types"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// Service stores context of Service object
//
type Service struct {
	credentials *credentials.CredentialsJSON
	projectID   string
}

func NewService(credentialsJson *credentials.CredentialsJSON, projectID string) *Service {
	return &Service{
		credentials: credentialsJson,
		projectID:   projectID,
	}
}

// isValid checks whether necessary credentials file and projectid are set
func (service *Service) isValid() *errortools.Error {
	if service.credentials == nil || service.credentials == new(credentials.CredentialsJSON) || service.projectID == "" {
		return errortools.ErrorMessage("Service CredentialsFile and/or ProjectID not set.")
	}

	return nil
}

// CreateClient creates client object for Service
//
func (service *Service) CreateClient() (*bigquery.Client, *errortools.Error) {
	e := service.isValid()
	if e != nil {
		return nil, e
	}

	ctx := context.Background()

	credJSON, err := json.Marshal(service.credentials)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	cl, err := bigquery.NewClient(ctx, service.projectID, option.WithCredentialsJSON(credJSON))
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return cl, nil
}

// TableExists checks whether or not specified table exists in Service
//
func (service *Service) GetTables(client *bigquery.Client, datasetName string) (*[]bigquery.Table, *errortools.Error) {
	if client == nil {
		_client, err := service.CreateClient()
		if err != nil {
			return nil, err
		}

		client = _client
	}

	err := service.isValid()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	tables := []bigquery.Table{}

	it := client.Dataset(datasetName).Tables(ctx)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if table != nil {
			tables = append(tables, *table)
		}
	}

	return &tables, nil
}

// TableExists checks whether or not specified table exists in Service
//
func (service *Service) TableExists(client *bigquery.Client, datasetName string, tableName string) (bool, *errortools.Error) {
	if client == nil {
		_client, err := service.CreateClient()
		if err != nil {
			return false, err
		}

		client = _client
	}

	err := service.isValid()
	if err != nil {
		return false, err
	}

	ctx := context.Background()

	it := client.Dataset(datasetName).Tables(ctx)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if table.TableID == tableName {
			return true, nil
		}
	}

	return false, nil
}

// CreateTable : creates table based on passed struct scheme
//
func (service *Service) CreateTable(client *bigquery.Client, datasetName string, tableName string, schema interface{}, recreate bool) (*bigquery.Table, *errortools.Error) {
	if client == nil {
		_client, e := service.CreateClient()
		if e != nil {
			return nil, e
		}

		client = _client
	}

	e := service.isValid()
	if e != nil {
		return nil, e
	}

	ctx := context.Background()

	dataset := client.Dataset(datasetName)
	table := dataset.Table(tableName)

	// check whether table exists
	tableExists, errExists := service.TableExists(client, datasetName, tableName)
	if errExists != nil {
		return table, errExists
	}

	if tableExists && recreate {
		// delete previous table
		err := table.Delete(ctx)
		if err != nil {
			return table, errortools.ErrorMessage(err)
		}
	}

	if !tableExists || recreate {
		// create schema for temp table
		schema1, err := bigquery.InferSchema(schema)
		if err != nil {
			return table, errortools.ErrorMessage(err)
		}

		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema1}); err != nil {
			return table, errortools.ErrorMessage(err)
		}

		count := 0
		tableExists, e = service.TableExists(client, datasetName, tableName)
		if errExists != nil {
			return table, e
		}
		for {
			if count > 1000 || tableExists {
				break
			}

			tableExists, e = service.TableExists(client, datasetName, tableName)
			if errExists != nil {
				return table, e
			}

			count++
		}
	}

	return table, nil
}

func (service *Service) DeleteTable(client *bigquery.Client, datasetName string, tableName string) *errortools.Error {
	if client == nil {
		_client, err := service.CreateClient()
		if err != nil {
			return err
		}

		client = _client
	}

	dataset := client.Dataset(datasetName)

	if dataset == nil {
		return errortools.ErrorMessage(fmt.Sprintf("Dataset %s does not exist.", datasetName))
	}

	table := dataset.Table(tableName)

	if table == nil {
		return errortools.ErrorMessage(fmt.Sprintf("Table %s does not exist in dataset %s.", tableName, datasetName))
	}

	err := table.Delete(context.Background())
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

// Run is a generic function that runs the passed sql query in Service
//
func (service *Service) Run(client *bigquery.Client, sql string, pendingMessage string) *errortools.Error {
	if client == nil {
		_client, err := service.CreateClient()
		if err != nil {
			return err
		}

		client = _client
	}

	e := service.isValid()
	if e != nil {
		return e
	}

	ctx := context.Background()
	//fmt.Println(sql)

	q := client.Query(sql)

	job, err := q.Run(ctx)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	fmt.Printf("\n%s...", pendingMessage)
	defer fmt.Printf("\n")

	for {
		status, err := job.Status(ctx)
		if err != nil {
			return errortools.ErrorMessage(err)
		}
		if status.Done() {
			if status.Err() != nil {
				return errortools.ErrorMessage(status.Err())
				//log.Fatalf("Job failed with error %v", status.Err())
			}
			break
		}
		fmt.Printf(" ...")
		time.Sleep(1 * time.Second)
	}

	return nil
}

// Insert : generic function to batchwise stream data to a Service table
//
func (service *Service) Insert(table *bigquery.Table, array []interface{}) *errortools.Error {
	err := service.isValid()
	if err != nil {
		return err
	}

	ctx := context.Background()

	//array := data.GetInterfaceArray()

	ins := table.Inserter()

	batchSize := 1000
	slice := array

	for {
		len := len(slice)

		//fmt.Printf("\nlen: %v", len)

		if len == 0 {
			break
		}

		if len < batchSize {
			batchSize = len
		}

		err := ins.Put(ctx, slice[:batchSize])
		if err != nil {
			return errortools.ErrorMessage(err)
		}

		slice = slice[batchSize:]
	}

	return nil
}

// InsertSlice : generic function to batchwise stream array into Service table
//
func (service *Service) InsertSlice(datasetName string, slice []interface{}, model interface{}, tableName string) *errortools.Error {
	err := service.isValid()
	if err != nil {
		return err
	}

	client, errClient := service.CreateClient()
	if errClient != nil {
		return errClient
	}

	if tableName == "" {
		guid := types.NewGUID()
		tableName = "temp_" + strings.Replace(guid.String(), "-", "", -1)
	}

	table, errTable := service.CreateTable(client, datasetName, tableName, model, false)
	if errTable != nil {
		return errTable
	}

	errInsert := service.Insert(table, slice)
	if errInsert != nil {
		return errInsert
	}

	return nil
}

// Select returns RowIterator from arbitrary select_ query (was: Get)
//
func (service *Service) Select(selectConfig *SelectConfig) (*bigquery.RowIterator, *errortools.Error) {
	sqlSelect := selectConfig.SQLSelect
	if sqlSelect == "" {
		sqlSelect = "*"
	}

	sqlWhere := selectConfig.SQLWhere
	if sqlWhere != "" {
		if !strings.HasSuffix(strings.ToUpper(sqlWhere), "WHERE ") {
			sqlWhere = "WHERE " + sqlWhere
		}
	}

	sqlOrderBy := ""
	if selectConfig.SQLOrderBy != nil {
		sqlOrderBy = *selectConfig.SQLOrderBy
	}
	if sqlOrderBy != "" {
		if !strings.HasSuffix(strings.ToUpper(sqlOrderBy), "ORDER BY ") {
			sqlOrderBy = "ORDER BY " + sqlOrderBy
		}
	}

	sqlLimit := ""
	if selectConfig.SQLLimit != nil {
		sqlLimit = fmt.Sprintf("LIMIT %v", *selectConfig.SQLLimit)
	}

	sql := "SELECT " + sqlSelect + " FROM `" + selectConfig.DatasetName + "." + selectConfig.TableOrViewName + "` " + sqlWhere + " " + sqlOrderBy + " " + sqlLimit
	//fmt.Println(sql)

	return service.select_(sql)
}

// SelectRaw returns RowIterator from arbitrary select_ query (was: Get)
//
func (service *Service) SelectRaw(sql string) (*bigquery.RowIterator, *errortools.Error) {
	return service.select_(sql)
}

// select_ returns RowIterator from arbitrary select_ query
//
func (service *Service) select_(sql string) (*bigquery.RowIterator, *errortools.Error) {
	e := service.isValid()
	if e != nil {
		return nil, e
	}

	client, e := service.CreateClient()
	if e != nil {
		return nil, e
	}

	ctx := context.Background()

	q := client.Query(sql)

	it, err := q.Read(ctx)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return it, nil
}

// Delete deletes rows from table
//
func (service *Service) Delete(datasetName string, tableName string, sqlWhere string) *errortools.Error {
	//sqlWhere = strings.Trim(strings.ToLower(sqlWhere), " ")

	if sqlWhere != "" {
		if !strings.HasSuffix(sqlWhere, "where ") {
			sqlWhere = "WHERE " + sqlWhere
		}
	}

	sql := "DELETE FROM `" + datasetName + "." + tableName + "` " + sqlWhere

	//fmt.Println(sql)

	return service.Run(nil, sql, "deleting")
}

// Merge runs merge query in Service, schema contains the table schema which needs to match the Service table.
// All properties of model with suffix 'Json' will be ignored. All rows with Ignore = TRUE will be ignored as well.
//
func (service *Service) Merge(schema interface{}, sourceTable string, targetTable string, idField string, hasIgnoreField bool) *errortools.Error {
	var sqlUpdate, sqlInsert, sqlValues string = ``, ``, ``

	v := reflect.ValueOf(schema)
	vType := v.Type()

	for i := 0; i < v.NumField(); i++ {
		fieldName := vType.Field(i).Name

		if !strings.HasSuffix(fieldName, "Json") && fieldName != "Ignore" {
			// fieldNames ending with "Json" should not be imported

			fieldName = "`" + fieldName + "`"

			if i > 0 {
				sqlUpdate += ","
				sqlInsert += ","
				sqlValues += ","
			}
			sqlUpdate += "TARGET." + fieldName + " = SOURCE." + fieldName
			sqlInsert += fieldName
			sqlValues += "SOURCE." + fieldName
		}
	}

	sql := "MERGE `" + targetTable + "` AS TARGET"
	sql += " USING `" + sourceTable + "` AS SOURCE"
	sql += " ON TARGET." + idField + " = SOURCE." + idField
	sql += " WHEN MATCHED"
	if hasIgnoreField {
		sql += " AND SOURCE.Ignore IS FALSE"
	}
	sql += " THEN UPDATE SET " + sqlUpdate
	sql += " WHEN NOT MATCHED BY TARGET"
	if hasIgnoreField {
		sql += " AND SOURCE.Ignore IS FALSE"
	}
	sql += " THEN INSERT(" + sqlInsert + ") VALUES(" + sqlValues + ")"

	return service.Run(nil, sql, "merging")
}

// GetValue returns one single value from query
//
func (service *Service) GetValue(selectConfig *SelectConfig) (string, *errortools.Error) {
	it, err := service.Select(selectConfig)
	if err != nil {
		return "", err
	}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			//return "", nil
			break
		}
		if err != nil {
			return "", errortools.ErrorMessage(err)
		}

		if values[0] == nil {
			return "", nil
		} else {
			return fmt.Sprintf("%s", values[0]), nil
		}
	}

	return "", nil
}

// GetValues returns multiple values from query
//
func (service *Service) GetValues(selectConfig *SelectConfig) (*[]string, *errortools.Error) {
	it, err := service.Select(selectConfig)
	if err != nil {
		return nil, err
	}

	values_ := []string{}

	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			//return nil, nil
			break
		}
		if err != nil {
			return nil, errortools.ErrorMessage(err)
		}

		for _, value := range values {
			values_ = append(values_, fmt.Sprintf("%v", value))
		}
	}

	return &values_, nil
}

type SelectConfig struct {
	DatasetName     string
	TableOrViewName string
	SQLSelect       string
	SQLWhere        string
	SQLOrderBy      *string
	SQLLimit        *uint64
	Model           interface{}
}

// GetStruct returns struct from query
//
func (service *Service) GetStruct(selectConfig *SelectConfig, model interface{}) (uint64, *errortools.Error) {
	if selectConfig == nil {
		return 0, errortools.ErrorMessage("SelectConfig must be a non-nil pointer")
	}

	it, e := service.Select(selectConfig)
	if e != nil {
		return 0, e
	}

	for {
		err := it.Next(model)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, errortools.ErrorMessage(err)
		}

		break
	}

	return it.TotalRows, nil
}

// CopyObjectToTable copies content of GCS object to table
//
func (service *Service) CopyObjectToTable(obj *storage.ObjectHandle, datasetName string, tableName string, schema interface{}, ctx context.Context, truncateTable bool, deleteObject bool) *errortools.Error {
	client, e := service.CreateClient()
	if e != nil {
		return e
	}

	//if rowCount > 0 {
	// get GCSReference
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", obj.BucketName(), obj.ObjectName()))

	// set FileConfig attribute of GCSReference struct
	var dataFormat bigquery.DataFormat
	dataFormat = "NEWLINE_DELIMITED_JSON"
	schema1, err := bigquery.InferSchema(schema)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	flConfig := bigquery.FileConfig{SourceFormat: dataFormat, Schema: schema1}
	gcsRef.FileConfig = flConfig

	// load data from GCN object to Service
	loader := client.Dataset(datasetName).Table(tableName).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	tableWriteDisposition := bigquery.WriteAppend
	if truncateTable {
		tableWriteDisposition = bigquery.WriteTruncate
	}
	loader.WriteDisposition = tableWriteDisposition

	job, err := loader.Run(ctx)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	// poll until ready
	pollInterval := 5 * time.Second

	for {
		status, err := job.Status(ctx)
		if err != nil {
			return errortools.ErrorMessage(err)
		}

		if status.Done() {
			if status.Err() != nil {
				for _, e := range status.Errors {
					fmt.Println(e.Message)
				}

				return errortools.ErrorMessage(fmt.Sprintf("Job failed with error %v", status.Err()))
			}
			break
		}
		time.Sleep(pollInterval)
	}
	//}

	if deleteObject {
		// delete GCS object
		err = obj.Delete(ctx)
		if err != nil {
			return errortools.ErrorMessage(err)
		}
	}

	return nil
}

// type conversion functions

func IntToNullInt64(i *int) bigquery.NullInt64 {
	ii := bigquery.NullInt64{0, false}
	if i != nil {
		ii = bigquery.NullInt64{int64(*i), true}
	}

	return ii
}

func Int32ToNullInt64(i *int32) bigquery.NullInt64 {
	ii := bigquery.NullInt64{0, false}
	if i != nil {
		ii = bigquery.NullInt64{int64(*i), true}
	}

	return ii
}

func Int64ToNullInt64(i *int64) bigquery.NullInt64 {
	ii := bigquery.NullInt64{0, false}
	if i != nil {
		ii = bigquery.NullInt64{*i, true}
	}

	return ii
}

func NullInt64ToInt(i bigquery.NullInt64) *int64 {
	if i.Valid {
		return &i.Int64
	} else {
		return nil
	}
}

func Float64ToNullFloat64(i *float64) bigquery.NullFloat64 {
	ii := bigquery.NullFloat64{0, false}
	if i != nil {
		ii = bigquery.NullFloat64{float64(*i), true}
	}

	return ii
}

func NullFloat64ToFloat64(i bigquery.NullFloat64) *float64 {
	if i.Valid {
		return &i.Float64
	} else {
		return nil
	}
}

func StringToNullString(i *string) bigquery.NullString {
	ii := bigquery.NullString{"", false}
	if i != nil {
		ii = bigquery.NullString{*i, true}
	}

	return ii
}

func NullStringToString(i bigquery.NullString) *string {
	if i.Valid {
		return &i.StringVal
	} else {
		return nil
	}
}

func TimeToNullTimestamp(t *time.Time) bigquery.NullTimestamp {
	ts := bigquery.NullTimestamp{time.Now(), false}
	if t != nil {
		if !t.IsZero() {
			ts = bigquery.NullTimestamp{*t, true}
		}
	}

	return ts
}

func NullTimestampToTime(i bigquery.NullTimestamp) *time.Time {
	if i.Valid {
		return &i.Timestamp
	} else {
		return nil
	}
}

func DateToNullTimestamp(d *types.Date) bigquery.NullTimestamp {
	ts := bigquery.NullTimestamp{time.Now(), false}
	if d != nil {
		if !d.IsZero() {
			ts = bigquery.NullTimestamp{d.Time, true}
		}
	}

	return ts
}

func DateToNullDate(d *civil.Date) bigquery.NullDate {
	dd := bigquery.NullDate{civil.Date{}, false}
	if d != nil {
		if d.IsValid() {
			dd = bigquery.NullDate{*d, true}
		}
	}

	return dd
}

func TimeToNullDate(t *time.Time) bigquery.NullDate {
	dd := bigquery.NullDate{civil.Date{}, false}
	if t != nil {
		if !t.IsZero() {
			dd = bigquery.NullDate{civil.Date{t.Year(), t.Month(), t.Day()}, true}
		}
	}

	return dd
}

func TimeToNullTime(t *time.Time) bigquery.NullTime {
	tt := bigquery.NullTime{civil.Time{}, false}
	if t != nil {
		if !t.IsZero() {
			tt = bigquery.NullTime{civil.TimeOf(*t), true}
		}
	}

	return tt
}

func TimeToNullDateTime(t *time.Time) bigquery.NullDateTime {
	tt := bigquery.NullDateTime{civil.DateTime{civil.Date{}, civil.Time{}}, false}
	if t != nil {
		if !t.IsZero() {
			tt = bigquery.NullDateTime{civil.DateTimeOf(*t), true}
		}
	}

	return tt
}

func TimeCivilToNullTime(t *civil.Time) bigquery.NullTime {
	tt := bigquery.NullTime{civil.Time{}, false}
	if t != nil {
		if t.IsValid() {
			tt = bigquery.NullTime{*t, true}
		}
	}

	return tt
}

func BoolToNullBool(b *bool) bigquery.NullBool {
	bb := bigquery.NullBool{false, false}
	if b != nil {
		bb = bigquery.NullBool{*b, true}
	}

	return bb
}
