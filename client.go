package dynamo_document_client

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"reflect"
	"sync"
)

type DynamoDBApi interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type DocumentClient struct {
	table string
	db    DynamoDBApi
}

var db *DocumentClient

var lock = &sync.Mutex{}

func NewClient(table string, opt Options) *DocumentClient {
	lock.Lock()
	defer lock.Unlock()

	if db == nil {
		db = initDb(table, opt)
	}

	return db
}

type Options struct {
	DB     func(o *dynamodb.Options)
	Config func(o *config.LoadOptions) error
}

func initDb(table string, opt Options) *DocumentClient {
	cfg, err := config.LoadDefaultConfig(context.TODO(), opt.Config)
	if err != nil {
		panic(err)
	}
	client := dynamodb.NewFromConfig(cfg, opt.DB)

	return &DocumentClient{
		table: table,
		db:    client,
	}
}

func (r *DocumentClient) Get(key map[string]types.AttributeValue, t interface{}) error {
	if reflect.ValueOf(t).Kind() != reflect.Ptr {
		return errors.New("target is not a pointer")
	}
	resp, err := r.db.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(r.table),
		Key:       key,
	})
	if err != nil {
		return err
	}
	if resp.Item == nil {
		return errors.New("item not found")
	}
	if err = attributevalue.UnmarshalMap(resp.Item, t); err != nil {
		log.Println("GetItem error", err)
		return err
	}
	return nil
}

func (r *DocumentClient) Put(v interface{}) error {
	item, err := attributevalue.MarshalMap(v)
	if err != nil {
		return err
	}
	_, err = r.db.PutItem(context.TODO(), &dynamodb.PutItemInput{
		Item:      item,
		TableName: &r.table,
	})
	if err != nil {
		log.Println("PutItem error", err)
	}
	return err
}

func (r *DocumentClient) Delete(key map[string]types.AttributeValue) error {
	_, err := r.db.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: &r.table,
		Key:       key,
	})
	return err
}

func (r *DocumentClient) Scan(e expression.Expression) (ret []map[string]types.AttributeValue, err error) {
	return r.scan(e, nil)
}

func (r *DocumentClient) Query(e expression.Expression) (ret []map[string]types.AttributeValue, err error) {
	return r.query(e, nil)
}

func (r *DocumentClient) query(e expression.Expression, lastKey map[string]types.AttributeValue) (agg []map[string]types.AttributeValue, err error) {
	q := &dynamodb.QueryInput{
		TableName:                 &r.table,
		ExpressionAttributeNames:  e.Names(),
		ExpressionAttributeValues: e.Values(),
		KeyConditionExpression:    e.KeyCondition(),
		FilterExpression:          e.Filter(),
	}

	if lastKey != nil {
		q.ExclusiveStartKey = lastKey
	}

	resp, err := r.db.Query(context.TODO(), q)
	if err != nil {
		log.Println("Scan error", err)
		return nil, err
	}

	lk := resp.LastEvaluatedKey

	if len(resp.Items) > 0 {
		agg = append(agg, resp.Items...)
	}

	if len(lk) > 0 {
		var ee error
		a, ee := r.query(e, lk)
		if ee != nil {
			return nil, err
		}
		agg = append(agg, a...)
	}

	return agg, nil
}

func (r *DocumentClient) scan(e expression.Expression, lastKey map[string]types.AttributeValue) (agg []map[string]types.AttributeValue, err error) {
	q := &dynamodb.ScanInput{
		TableName:                 &r.table,
		ExpressionAttributeNames:  e.Names(),
		ExpressionAttributeValues: e.Values(),
		FilterExpression:          e.Filter(),
	}

	if lastKey != nil {
		q.ExclusiveStartKey = lastKey
	}

	resp, err := r.db.Scan(context.TODO(), q)
	if err != nil {
		log.Println("Scan error", err)
		return nil, err
	}

	lk := resp.LastEvaluatedKey

	if len(resp.Items) > 0 {
		agg = append(agg, resp.Items...)
	}

	if len(lk) > 0 {
		var ee error
		a, ee := r.scan(e, lk)
		if ee != nil {
			return nil, err
		}
		agg = append(agg, a...)
	}

	return agg, nil
}
