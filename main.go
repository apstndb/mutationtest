package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/code"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context) error {
	project := flag.String("project", "", "")
	instance := flag.String("instance", "", "")
	database := flag.String("database", "", "")

	batchCount := flag.Int("batch-count", 40000, "")
	batchSize := flag.Int("batch-size", 1, "")
	flag.Parse()

	databaseStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", *project, *instance, *database)

	cli, err := spanner.NewClientWithConfig(ctx, databaseStr, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{MinOpened: 1},
	})
	if err != nil {
		return err
	}
	defer cli.Close()

	type TableRow struct {
		PK  string `spanner:"PK"`
		Col int64  `spanner:"Col"`
	}

	{
		var mutationGroups []*spanner.MutationGroup
		for iBatchCount := 0; iBatchCount < *batchCount; iBatchCount++ {
			var mutations []*spanner.Mutation
			for iBatchSize := 0; iBatchSize < *batchSize; iBatchSize++ {
				mut, err := spanner.InsertOrUpdateStruct("MutationTest", TableRow{PK: uuid.NewString(), Col: int64(iBatchCount)})
				// mut, err := spanner.InsertStruct("MutationTest", TableRow{PK: "foo", Col: int64(i)})
				if err != nil {
					return err
				}
				mutations = append(mutations, mut)
			}
			mutationGroups = append(mutationGroups, &spanner.MutationGroup{Mutations: mutations})
		}

		it := cli.BatchWrite(ctx, mutationGroups)
		successBatchCount := 0
		failBatchCount := 0
		successCount := 0
		failCount := 0
		err := it.Do(func(r *spannerpb.BatchWriteResponse) error {
			if st := r.GetStatus(); st.GetCode() != int32(code.Code_OK) {
				failBatchCount++
				failCount += len(r.GetIndexes())
				log.Println(st)
			} else {
				log.Printf("len(Indexes): %d, CommitTimestamp: %v", len(r.GetIndexes()), r.GetCommitTimestamp().AsTime())
				successBatchCount++
				successCount += len(r.GetIndexes())
			}
			return nil
		})
		if err != nil {
			log.Printf("BatchWrite failed: %v", err)
		} else {
			log.Printf("BatchWrite success")
		}
		log.Printf("successBatchCount: %v, failBatchCount: %v, successCount: %v, failCount: %v", successBatchCount, failBatchCount, successCount, failCount)
	}
	return nil
}
