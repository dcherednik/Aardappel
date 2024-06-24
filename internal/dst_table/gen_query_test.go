package dst_table

import (
	"aardappel/internal/reader"
	"aardappel/internal/types"
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefferentLensOfKeys(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":15, \"value3\":null},\"key\":[15,\"15\"],\"ts\":[1718408057080,18446744073709551615]}"))
	_, err := GenQueryFromUpdateTx(ctx, "path", []string{"key1"}, txData)
	targetErr := "GenQueryFromUpdateTx: len of primary key is not equal to len of values"
	assert.Equal(t, err.Error(), targetErr)
}

func TestGenUpdateQuery(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":15, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"15\"],\"ts\":[1718408057080,18446744073709551615]}"))
	query, err := GenQueryFromUpdateTx(ctx, "path", []string{"key1", "key2"}, txData)
	assert.Equal(t, nil, err)
	assert.Equal(t, "UPSERT INTO path (key1, key2, value1, value2, value3, value4) VALUES (15, \"15\", \"15\", 15, 1.00000009, null);\n", query)
}

func TestGenEraseQuery(t *testing.T) {
	ctx := context.Background()
	txData, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[15,\"15\"],\"ts\":[1718408057080,18446744073709551615]}"))
	query, err := GenQueryFromEraseTx(ctx, "path", []string{"key1", "key2"}, txData)
	assert.Equal(t, nil, err)
	assert.Equal(t, "DELETE FROM path WHERE key1 = 15, key2 = \"15\";\n", query)
}

func TestGenMultiQuery(t *testing.T) {
	ctx := context.Background()
	txData1, _ := reader.ParseTxData(ctx, []byte("{\"update\":{\"value1\":\"15\", \"value2\":15, \"value3\":1.00000009, \"value4\":null},\"key\":[15,\"15\"],\"ts\":[1718408057080,18446744073709551615]}"))
	txData2, _ := reader.ParseTxData(ctx, []byte("{\"erase\":{},\"key\":[15,\"15\"],\"ts\":[1718408057080,18446744073709551615]}"))
	txData3, _ := reader.ParseTxData(ctx, []byte("{\"update\":{},\"key\":[16, 16],\"ts\":[1718408057080,18446744073709551615]}"))
	query, err := GenQuery(ctx, "path", []string{"key1", "key2"}, []types.TxData{txData1, txData2, txData3})
	assert.Equal(t, nil, err)
	expectedResult := "UPSERT INTO path (key1, key2, value1, value2, value3, value4) VALUES (15, \"15\", \"15\", 15, 1.00000009, null);\n" +
		"DELETE FROM path WHERE key1 = 15, key2 = \"15\";\n" +
		"UPSERT INTO path (key1, key2) VALUES (16, 16);\n"
	assert.Equal(t, expectedResult, query.Query)
}
