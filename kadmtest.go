//go:build ignore

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"

	//"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	seeds := []string{"vm-kafka-ump01tn.mbrd.ru:9092", "vm-kafka-ump02tn.mbrd.ru:9092", "vm-kafka-ump03tn.mbrd.ru:9092"}

	pass := "XXXXX"
	user := "XXXXX"

	var adminClient *kadm.Client
	{
		client, err := kgo.NewClient(
			kgo.SeedBrokers(seeds...),
			// kgo.SASL((scram.Auth{User: user, Pass: pass}).AsSha512Mechanism()),
			kgo.SASL((plain.Auth{User: user, Pass: pass}).AsMechanism()),

			// Do not try to send requests newer than 2.4.0 to avoid breaking changes in the request struct.
			// Sometimes there are breaking changes for newer versions where more properties are required to set.
			kgo.MaxVersions(kversion.V2_4_0()),
		)
		if err != nil {
			panic(err)
		}
		defer client.Close()

		adminClient = kadm.NewClient(client)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dg, err := adminClient.DescribeGroups(ctx, "interestrate_loader")
	if err != nil {
		die("failed to describe group: %v", err)
	}

	for _, m := range dg["interestrate_loader"].Members {
		mc, _ := m.Assigned.AsConsumer()
		for _, mt := range mc.Topics {
			for _, p := range mt.Partitions {
				fmt.Printf("client:%s\tpartitions: %d\n", m.ClientID, p)
			}
		}
	}
}
