package processor

import (
	"aardappel/internal/hb_tracker"
	"aardappel/internal/tx_queue"
	"aardappel/internal/types"
	"aardappel/internal/util/xlog"
	"context"
	"go.uber.org/zap"
	"time"
)

type Processor struct {
	txChannel chan func() error
	hbTracker hb_tracker.HeartBeatTracker
	txQueue   tx_queue.TxQueue
}

type Channel interface {
	EnqueueTx(data types.TxData)
	EnqueueHb(heartbeat types.HbData)
}

func (processor *Processor) EnqueueHb(hb types.HbData) {
	processor.txChannel <- func() error {
		return processor.hbTracker.AddHb(hb)
	}
}

func (processor *Processor) EnqueueTx(tx types.TxData) {
	processor.txChannel <- func() error {
		processor.txQueue.PushTx(tx)
		return nil
	}
}

func (processor *Processor) FormatTx(ctx context.Context) error {
	var hb types.HbData
	var maxEventPerIteration int = 100
	for {
		for maxEventPerIteration > 0 {
			maxEventPerIteration--

			select {
			case fn := <-processor.txChannel:
				err := fn()
				if err != nil {
					return err
				}
				continue
			default:
				break
			}
		}
		var ok bool
		hb, ok = processor.hbTracker.GetReady()
		if ok {
			break
		}
		//TODO: Wait hb here
		time.Sleep(1 * time.Millisecond)
	}
	// Here we have heartbeat and filled TxQueue - ready to format TX
	txs := processor.txQueue.GetTxs(hb.Step)
	for _, data := range txs {
		xlog.Debug(ctx, "Parsed tx data",
			zap.Any("column_values", data.ColumnValues),
			zap.String("operation_type", data.OperationType.String()),
			zap.Any("key", data.KeyValues),
			zap.Uint64("step", data.Step),
			zap.Uint64("tx_id", data.TxId))
	}
}
