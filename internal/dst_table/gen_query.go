package dst_table

import (
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"sort"
	"strings"
)

type KeyValue struct {
	Column string
	Value  interface{}
}

func GetSortColumns(keyValuesMap map[string]interface{}) []KeyValue {
	result := make([]KeyValue, 0, len(keyValuesMap))
	for column, value := range keyValuesMap {
		result = append(result, KeyValue{Column: column, Value: value})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Column < result[j].Column
	})

	return result
}

func GenQueryFromUpdateTx(ctx context.Context, tablePath string, primaryKeys []string, txData types.TxData) (string, error) {
	var result string
	if len(primaryKeys) != len(txData.KeyValues) {
		xlog.Error(ctx, "Len of primary key is not equal to len of values",
			zap.Int("len of primary keys", len(primaryKeys)),
			zap.Int("len of values", len(txData.KeyValues)))
		return "", fmt.Errorf("GenQueryFromUpdateTx: len of primary key is not equal to len of values")
	}
	result = "UPSERT INTO " + tablePath + " (" + strings.Join(primaryKeys, ", ")
	columnValues := GetSortColumns(txData.ColumnValues)
	for i := range columnValues {
		result += ", "
		result += columnValues[i].Column
	}
	result += ") VALUES ("
	for i := range txData.KeyValues {
		jsonData, err := json.Marshal(txData.KeyValues[i])
		if err != nil {
			xlog.Error(ctx, "Can't parse key value", zap.Error(err))
			return "", fmt.Errorf("GenQueryFromUpdateTx: %w", err)
		}
		result += string(jsonData)
		if i != len(primaryKeys)-1 {
			result += ", "
		}
	}
	for i := range columnValues {
		result += ", "
		jsonData, err := json.Marshal(columnValues[i].Value)
		if err != nil {
			xlog.Error(ctx, "Can't parse column value", zap.Error(err))
			return "", fmt.Errorf("GenQueryFromUpdateTx: %w", err)
		}
		result += string(jsonData)
	}
	result += ");\n"
	xlog.Debug(ctx, "Gen upsert query ", zap.String("query", result))
	return result, nil
}

func GenQueryFromEraseTx(ctx context.Context, tablePath string, primaryKeys []string, txData types.TxData) (string, error) {
	var result string
	if len(primaryKeys) != len(txData.KeyValues) {
		xlog.Error(ctx, "Len of primary key is not equal to len of values",
			zap.Int("len of primary keys", len(primaryKeys)),
			zap.Int("len of values", len(txData.KeyValues)))
		return "", fmt.Errorf("GenQueryFromEraseTx: len of primary key is not equal to len of values")
	}
	result = "DELETE FROM " + tablePath + " WHERE "
	for i := range primaryKeys {
		jsonData, err := json.Marshal(txData.KeyValues[i])
		if err != nil {
			xlog.Error(ctx, "Can't parse key value", zap.Error(err))
			return "", fmt.Errorf("GenQueryFromEraseTx: %w", err)
		}
		result += primaryKeys[i] + " = " + string(jsonData)
		if i != len(primaryKeys)-1 {
			result += ", "
		} else {
			result += ";\n"
		}
	}
	return result, nil
}

func GenQueryFromTx(ctx context.Context, tablePath string, primaryKeys []string, txData types.TxData) (string, error) {
	var result string
	if txData.IsUpdateOperation() {
		return GenQueryFromUpdateTx(ctx, tablePath, primaryKeys, txData)
	}
	if txData.IsEraseOperation() {
		return GenQueryFromEraseTx(ctx, tablePath, primaryKeys, txData)
	}
	xlog.Error(ctx, "GenQueryFromTx: unknown data tx operation type", zap.Any("tx_data", txData))
	return result, fmt.Errorf("GenQueryFromTx: unknown data tx operation type")
}

func GenQuery(ctx context.Context, tablePath string, primaryKeys []string, txData []types.TxData) (PushQuery, error) {
	var result PushQuery
	for i := range txData {
		txQuery, err := GenQueryFromTx(ctx, tablePath, primaryKeys, txData[i])
		if err != nil {
			xlog.Error(ctx, "Can't gen query", zap.Any("tx_data", txData[i]))
			return PushQuery{}, fmt.Errorf("GenQuery: %w", err)
		}
		result.Query += txQuery
	}
	xlog.Debug(ctx, "Gen multi query", zap.String("query", result.Query))
	return result, nil
}
