package main

import (
	configInit "aardappel/internal/config"
	"aardappel/internal/dst_table"
	processor "aardappel/internal/processor"
	topicReader "aardappel/internal/reader"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"flag"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"go.uber.org/zap"
	"time"
)

func main() {
	var confPath string

	flag.StringVar(&confPath, "config", "config.yaml", "aardappel configuration file")
	flag.Parse()

	ctx := context.Background()

	// Setup logging
	logger := xlog.SetupLogging(true)
	xlog.SetInternalLogger(logger)
	defer logger.Sync()

	// Setup config
	config, err := configInit.InitConfig(ctx, confPath)
	if err != nil {
		xlog.Fatal(ctx, "Unable to initialize config", zap.Error(err))
	}
	confStr, err := config.ToString()
	if err == nil {
		xlog.Debug(ctx, "Use configuration file",
			zap.String("config_path", confPath),
			zap.String("config", confStr))
	}

	opts := []ydb.Option{
		ydb.WithAccessTokenCredentials("root@builtin"),
	}

	// Connect to YDB
	srcDb, err := ydb.Open(ctx, config.SrcConnectionString, opts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to src cluster", zap.Error(err))
	}
	xlog.Debug(ctx, "YDB opened")

	dstDb, err := ydb.Open(ctx, config.DstConnectionString, opts...)
	if err != nil {
		xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	}

	var totalPartitions int
	for i := 0; i < len(config.Streams); i++ {
		desc, err := srcDb.Topic().Describe(ctx, config.Streams[i].SrcTopic)
		if err != nil {
			xlog.Fatal(ctx, "Unable to describe topic",
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		totalPartitions += len(desc.Partitions)
	}

	xlog.Debug(ctx, "All topics described",
		zap.Int("total parts", totalPartitions))

	prc := processor.NewProcessor(totalPartitions)
	var dstTables []*dst_table.DstTable
	for i := 0; i < len(config.Streams); i++ {
		reader, err := srcDb.Topic().StartReader(config.Streams[i].Consumer, topicoptions.ReadTopic(config.Streams[i].SrcTopic))
		if err != nil {
			xlog.Fatal(ctx, "Unable to create topic reader",
				zap.String("consumer", config.Streams[i].Consumer),
				zap.String("src_topic", config.Streams[i].SrcTopic),
				zap.Error(err))
		}
		dstTables = append(dstTables, dst_table.NewDstTable(dstDb.Table(), config.Streams[i].DstTable))
		err = dstTables[i].Init(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to init dst table")
		}
		xlog.Debug(ctx, "Start reading")
		go topicReader.ReadTopic(ctx, uint32(i), reader, prc)
	}

	time.Sleep(5 * time.Second)

	for {
		txDataPerTable := make([][]types.TxData, len(dstTables))
		batch, err := prc.FormatTx(ctx)
		if err != nil {
			xlog.Fatal(ctx, "Unable to format tx for destination")
		}
		for i := 0; i < len(batch.TxData); i++ {
			txDataPerTable[batch.TxData[i].TableId] = append(txDataPerTable[batch.TxData[i].TableId], batch.TxData[i])
		}
		if len(txDataPerTable) != len(dstTables) {
			xlog.Fatal(ctx, "Size of dstTables and tables in the tx mismatched",
				zap.Int("txDataPertabe", len(txDataPerTable)),
				zap.Int("dstTable", len(dstTables)))
		}
		for i := 0; i < len(txDataPerTable); i++ {
			query, err := dstTables[i].GenQuery(ctx, txDataPerTable[i])
			if err != nil {
				xlog.Fatal(ctx, "Unable to generate query")
			}

			xlog.Debug(ctx, "Query to perform", zap.String("query", query.Query))
		}
	}

	//db, err := ydb.Open(ctx, config.DstConnectionString)
	//if err != nil {
	//	xlog.Fatal(ctx, "Unable to connect to dst cluster", zap.Error(err))
	//}

	//client := db.Table()
	//for i := 0; i < len(config.Streams); i++ {
	//	dstTable := dst_table.NewDstTable(client, config.Streams[i].DstTable)
	//	err := dstTable.Init(ctx)
	//	if err != nil {
	//		xlog.Fatal(ctx, "Unable to init dst table", zap.Error(err))
	//	}
	//}

	//client := db.Table()

	// Perform YDB operation
	//err = ydb_operations.SomeYdbOperation(ctx, client)
	//if err != nil {
	//	xlog.Error(ctx, "ydb operation error", zap.Error(err))
	//	return
	//}

	// Create and print a protobuf message
	//var x protos.SomeMessage
	//x.Port = 123
	//s, err := prototext.Marshal(&x)
	//if err != nil {
	//	xlog.Error(ctx, "protobuf marshal error", zap.Error(err))
	//	return
	//}
	//xlog.Info(ctx, "protobuf message", zap.String("message", string(s))
}
