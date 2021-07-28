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

type SQLConfig struct {
	DatasetName      string
	TableOrViewName  *string
	TableOrViewAlias *string
	SQLSelect        *string
	SQLWhere         *string
	SQLOrderBy       *string
	SQLLimit         *uint64
	ModelOrSchema    interface{}
}

func (sqlConfig SQLConfig) GenerateTempTable() SQLConfig {
	guid := types.NewGUID()
	tableName := fmt.Sprintf("temp_%s", strings.Replace(guid.String(), "-", "", -1))

	sqlConfig.TableOrViewName = &tableName

	return sqlConfig
}

func (sqlConfig *SQLConfig) FullTableName() string {
	if sqlConfig == nil {
		return ""
	}
	if sqlConfig.TableOrViewName == nil {
		return ""
	}

	return fmt.Sprintf("%s.%s", sqlConfig.DatasetName, *sqlConfig.TableOrViewName)
}

// Service stores context of Service object
//
type Service struct {
	bigQueryClient *bigquery.Client
	context        context.Context
}

type ServiceConfig struct {
	CredentialsJSON *credentials.CredentialsJSON
	ProjectID       string
}

func NewService(serviceConfig *ServiceConfig) (*Service, *errortools.Error) {
	if serviceConfig == nil {
		return nil, errortools.ErrorMessage("ServiceConfig is nil pointer")
	}

	if serviceConfig.CredentialsJSON == nil {
		return nil, errortools.ErrorMessage("CredentialsJSON not provided")
	}

	if serviceConfig.ProjectID == "" {
		return nil, errortools.ErrorMessage("ProjectID not provided")
	}

	ctx := context.Background()

	credentialsByte, err := json.Marshal(serviceConfig.CredentialsJSON)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	client, err := bigquery.NewClient(ctx, serviceConfig.ProjectID, option.WithCredentialsJSON(credentialsByte))
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return &Service{
		bigQueryClient: client,
		context:        ctx,
	}, nil
}

func (service *Service) GetTables(sqlConfig *SQLConfig) (*[]bigquery.Table, *errortools.Error) {
	dataset, e := service.getDataset(sqlConfig)
	if e != nil {
		return nil, e
	}

	tables := []bigquery.Table{}

	it := dataset.Tables(service.context)

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

func (service *Service) TableExists(sqlConfig *SQLConfig) (bool, *errortools.Error) {
	dataset, tableHandle, e := service.getTableHandle(sqlConfig)
	if e != nil {
		return false, e
	}

	return service.tableExists(dataset, tableHandle)
}

func (service *Service) tableExists(dataset *bigquery.Dataset, tableHandle *bigquery.Table) (bool, *errortools.Error) {
	it := dataset.Tables(service.context)

	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if table.TableID == tableHandle.TableID {
			return true, nil
		}
	}

	return false, nil
}

func (service *Service) getDataset(sqlConfig *SQLConfig) (*bigquery.Dataset, *errortools.Error) {
	dataset := service.bigQueryClient.Dataset(sqlConfig.DatasetName)

	_, err := dataset.Metadata(service.context)
	if err != nil {
		fmt.Println(err)
		return nil, errortools.ErrorMessage(fmt.Sprintf("Dataset %s does not exist.", sqlConfig.DatasetName))
	}

	return dataset, nil
}

func (service *Service) getTableHandle(sqlConfig *SQLConfig) (*bigquery.Dataset, *bigquery.Table, *errortools.Error) {
	if sqlConfig.TableOrViewName == nil {
		return nil, nil, errortools.ErrorMessage("TableOrViewName is nil pointer")
	}

	dataset, e := service.getDataset(sqlConfig)
	if e != nil {
		return nil, nil, errortools.ErrorMessage(e)
	}

	return dataset, dataset.Table(*sqlConfig.TableOrViewName), nil
}

// CreateTable : creates table based on passed struct scheme
//
func (service *Service) CreateTable(sqlConfig *SQLConfig, data *[]interface{}, recreate bool) (*bigquery.Table, *errortools.Error) {
	dataset, tableHandle, e := service.getTableHandle(sqlConfig)
	if e != nil {
		return nil, e
	}

	// check whether table exists
	exists, e := service.tableExists(dataset, tableHandle)
	if e != nil {
		return nil, e
	}

	if exists && recreate {
		// delete previous table
		err := tableHandle.Delete(service.context)
		if err != nil {
			return tableHandle, errortools.ErrorMessage(err)
		}
	}

	if !exists || recreate {
		// create schema for temp table
		schema, err := bigquery.InferSchema(sqlConfig.ModelOrSchema)
		if err != nil {
			return tableHandle, errortools.ErrorMessage(err)
		}

		if err := tableHandle.Create(service.context, &bigquery.TableMetadata{Schema: schema}); err != nil {
			return tableHandle, errortools.ErrorMessage(err)
		}

		count := 0
		exists, e := service.tableExists(dataset, tableHandle)
		if e != nil {
			return nil, e
		}
		for {
			if count > 1000 || exists {
				break
			}

			exists, e = service.tableExists(dataset, tableHandle)
			if e != nil {
				return tableHandle, e
			}

			count++
		}
	}

	if data != nil {
		// insert data
		e = service.Insert(tableHandle, *data)
		if e != nil {
			return nil, e
		}
	}

	return tableHandle, nil
}

func (service *Service) DeleteTable(sqlConfig *SQLConfig) *errortools.Error {
	dataset, tableHandle, e := service.getTableHandle(sqlConfig)
	if e != nil {
		return e
	}

	// check whether table exists
	exists, e := service.tableExists(dataset, tableHandle)
	if e != nil {
		return e
	}

	if !exists {
		return errortools.ErrorMessage(fmt.Sprintf("Table %s does not exist in dataset %s.", *sqlConfig.TableOrViewName, sqlConfig.DatasetName))
	}

	err := tableHandle.Delete(context.Background())
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	return nil
}

// Run is a generic function that runs the passed sql query in Service
//
func (service *Service) Run(sql string, pendingMessage string) *errortools.Error {
	q := service.bigQueryClient.Query(sql)

	job, err := q.Run(service.context)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	fmt.Printf("\n%s...", pendingMessage)
	defer fmt.Printf("\n")

	for {
		status, err := job.Status(service.context)
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

		err := ins.Put(service.context, slice[:batchSize])
		if err != nil {
			return errortools.ErrorMessage(err)
		}

		slice = slice[batchSize:]
	}

	return nil
}

// InsertSlice : generic function to batchwise stream array into Service table
// REPLACED BY CreateTable(sqlConfig.Temptable(), slice, false)
/*func (service *Service) InsertSlice(sqlConfig *SQLConfig, tempTable bool, slice []interface{}) *errortools.Error {
	tableName := ""

	if tableName == "" {
		guid := types.NewGUID()
		tableName = "temp_" + strings.Replace(guid.String(), "-", "", -1)
	}

	table, errTable := service.CreateTable(sqlConfig, false)
	if errTable != nil {
		return errTable
	}

	errInsert := service.Insert(table, slice)
	if errInsert != nil {
		return errInsert
	}

	return nil
}*/

// Select returns RowIterator from arbitrary select_ query (was: Get)
//
func (service *Service) Select(sqlConfig *SQLConfig) (*bigquery.RowIterator, *errortools.Error) {
	if sqlConfig.TableOrViewName == nil {
		return nil, errortools.ErrorMessage("TableOrViewName is nil pointer")
	}

	_sqlSelect := "*"
	if sqlConfig.SQLSelect != nil {
		_sqlSelect = *sqlConfig.SQLSelect
	}

	_sqlAlias := ""
	if sqlConfig.TableOrViewAlias != nil {
		_sqlAlias = fmt.Sprintf("%s ", *sqlConfig.TableOrViewAlias)
	}

	_sqlWhere := ""
	if sqlConfig.SQLWhere != nil {
		if !strings.HasSuffix(strings.ToUpper(*sqlConfig.SQLWhere), "WHERE ") {
			_sqlWhere = fmt.Sprintf("WHERE %s", *sqlConfig.SQLWhere)
		}
	}

	_sqlOrderBy := ""
	if sqlConfig.SQLOrderBy != nil {
		_sqlOrderBy = *sqlConfig.SQLOrderBy
	}
	if _sqlOrderBy != "" {
		if !strings.HasSuffix(strings.ToUpper(_sqlOrderBy), "ORDER BY ") {
			_sqlOrderBy = fmt.Sprintf("ORDER BY %s", _sqlOrderBy)
		}
	}

	_sqlLimit := ""
	if sqlConfig.SQLLimit != nil {
		_sqlLimit = fmt.Sprintf("LIMIT %v", *sqlConfig.SQLLimit)
	}

	sql := "SELECT " + _sqlSelect + " FROM `" + sqlConfig.DatasetName + "." + *sqlConfig.TableOrViewName + "` " + _sqlAlias + " " + _sqlWhere + " " + _sqlOrderBy + " " + _sqlLimit
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
	q := service.bigQueryClient.Query(sql)

	it, err := q.Read(service.context)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	return it, nil
}

// Exists returns whether any arbitrary query returns any rows
//
func (service *Service) Exists(sqlConfig *SQLConfig) (bool, *errortools.Error) {
	it, e := service.Select(sqlConfig)
	if e != nil {
		return false, e
	}

	_ = it.Next(&[]bigquery.Value{}) //first call to Next needed to make TotalRows work

	if it.TotalRows == 0 {
		return false, nil
	}

	return true, nil
}

// Delete deletes rows from table
//
func (service *Service) Delete(sqlConfig *SQLConfig) *errortools.Error {
	if sqlConfig == nil {
		return errortools.ErrorMessage("sqlConfig is nil pointer")
	}

	if sqlConfig.TableOrViewName == nil {
		return errortools.ErrorMessage("sqlConfig.TableOrViewName is nil pointer")
	}

	sqlWhere := ""
	if sqlConfig.SQLWhere != nil {
		sqlWhere = *sqlConfig.SQLWhere
		if !strings.HasSuffix(sqlWhere, "where ") {
			sqlWhere = "WHERE " + sqlWhere
		}
	}

	sql := "DELETE FROM `" + sqlConfig.DatasetName + "." + *sqlConfig.TableOrViewName + "` " + sqlWhere

	//fmt.Println(sql)

	return service.Run(sql, "deleting")
}

// Merge runs merge query in Service, schema contains the table schema which needs to match the Service table.
// All properties of model with suffix 'Json' will be ignored. All rows with Ignore = TRUE will be ignored as well.
//
func (service *Service) Merge(sqlConfigSource *SQLConfig, sqlConfigTarget *SQLConfig, joinFields []string, doNotUpdateFields []string, hasIgnoreField bool) *errortools.Error {
	if sqlConfigSource == nil {
		return errortools.ErrorMessage("sqlConfigSource is nil pointer")
	}
	if sqlConfigTarget == nil {
		return errortools.ErrorMessage("sqlConfigTarget is nil pointer")
	}
	if len(joinFields) == 0 {
		return errortools.ErrorMessage("Specify at least one join field")
	}

	var sqlUpdate, sqlInsert, sqlValues string = ``, ``, ``

	v := reflect.ValueOf(sqlConfigTarget.ModelOrSchema)
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
			if !strings.Contains(fmt.Sprintf(";%s;", strings.ToLower(strings.Join(doNotUpdateFields, ";"))), fmt.Sprintf(";%s;", strings.ToLower(fieldName))) {
				sqlUpdate += "TARGET." + fieldName + " = SOURCE." + fieldName
			}
			sqlInsert += fieldName
			sqlValues += "SOURCE." + fieldName
		}
	}

	var sqlOn []string
	for _, joinField := range joinFields {
		sqlOn = append(sqlOn, fmt.Sprintf("TARGET.%s = SOURCE.%s", joinField, joinField))
	}

	sql := "MERGE `" + sqlConfigTarget.FullTableName() + "` AS TARGET"
	sql += " USING `" + sqlConfigSource.FullTableName() + "` AS SOURCE"
	sql += " ON " + strings.Join(sqlOn, " AND ")
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

	return service.Run(sql, "merging")
}

// GetValue returns one single value from query
//
func (service *Service) GetValue(sqlConfig *SQLConfig) (*bigquery.Value, *errortools.Error) {
	values, e := service.GetValues(sqlConfig)
	if e != nil {
		return nil, e
	}

	if len(*values) != 1 {
		return nil, errortools.ErrorMessagef("Row has %v columns instead of 1", len(*values))
	}

	return &(*values)[0], nil
}

// GetValues returns multiple values from query
//
func (service *Service) GetValues(sqlConfig *SQLConfig) (*[]bigquery.Value, *errortools.Error) {
	it, e := service.Select(sqlConfig)
	if e != nil {
		return nil, e
	}

	values := []bigquery.Value{}

	err := it.Next(&values)
	if err != nil {
		return nil, errortools.ErrorMessage(err)
	}

	if it.TotalRows != 1 {
		return nil, errortools.ErrorMessagef("Query returned %v rows instead of 1", it.TotalRows)
	}

	return &values, nil
}

// GetStruct returns struct from query
//
func (service *Service) GetStruct(sqlConfig *SQLConfig, model interface{}) (uint64, *errortools.Error) {
	if sqlConfig == nil {
		return 0, errortools.ErrorMessage("SQLConfig must be a non-nil pointer")
	}

	it, e := service.Select(sqlConfig)
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

type CopyObjectToTableConfig struct {
	ObjectHandle  *storage.ObjectHandle
	SQLConfig     *SQLConfig
	TruncateTable bool
	DeleteObject  bool
}

// CopyObjectToTable copies content of GCS object to table
//
func (service *Service) CopyObjectToTable(config *CopyObjectToTableConfig) *errortools.Error {
	if config == nil {
		return errortools.ErrorMessage("CopyObjectToTableConfig is nil pointer")
	}

	if config.SQLConfig == nil {
		return errortools.ErrorMessage("SQLConfig is nil pointer")
	}

	//if rowCount > 0 {
	// get GCSReference
	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", config.ObjectHandle.BucketName(), config.ObjectHandle.ObjectName()))

	// set FileConfig attribute of GCSReference struct
	var dataFormat bigquery.DataFormat
	dataFormat = "NEWLINE_DELIMITED_JSON"
	schema1, err := bigquery.InferSchema(config.SQLConfig.ModelOrSchema)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	flConfig := bigquery.FileConfig{SourceFormat: dataFormat, Schema: schema1}
	gcsRef.FileConfig = flConfig

	// load data from GCN object to Service
	_, tableHandle, e := service.getTableHandle(config.SQLConfig)
	if e != nil {
		return e
	}

	loader := tableHandle.LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	tableWriteDisposition := bigquery.WriteAppend
	if config.TruncateTable {
		tableWriteDisposition = bigquery.WriteTruncate
	}
	loader.WriteDisposition = tableWriteDisposition

	job, err := loader.Run(service.context)
	if err != nil {
		return errortools.ErrorMessage(err)
	}

	// poll until ready
	pollInterval := 5 * time.Second

	for {
		status, err := job.Status(service.context)
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

	if config.DeleteObject {
		// delete GCS object
		err = config.ObjectHandle.Delete(service.context)
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

// XML
func NullStringToXML(i bigquery.NullString) string {
	if i.Valid {
		return i.StringVal
	} else {
		return ""
	}
}

func NullInt64ToXML(i bigquery.NullInt64) string {
	if i.Valid {
		return fmt.Sprintf("%v", i.Int64)
	} else {
		return ""
	}
}

func NullFloat64ToXML(i bigquery.NullFloat64) string {
	if i.Valid {
		return fmt.Sprintf("%v", i.Float64)
	} else {
		return ""
	}
}

func NullBoolToXML(i bigquery.NullBool) string {
	if i.Valid {
		return fmt.Sprintf("%v", i.Bool)
	} else {
		return ""
	}
}

func NullTimestampToXML(i bigquery.NullTimestamp, layout string) string {
	if i.Valid {
		return i.Timestamp.Format(layout)
	} else {
		return ""
	}
}

func TimeToXML(i time.Time, layout string) string {
	if !i.IsZero() {
		return i.Format(layout)
	} else {
		return ""
	}
}
