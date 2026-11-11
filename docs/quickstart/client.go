// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
)

func main() {
	file := flag.String("config", "client.yaml", "SDK configuration file (not wp CLI cli.yaml)")
	flag.Parse()
	if err := run(*file); err != nil {
		log.Fatal(err)
	}
}

func run(file string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg, err := config.NewConfiguration(file)
	if err != nil {
		return fmt.Errorf("load SDK configuration: %w", err)
	}

	var client woodpecker.Client
	if cfg.Woodpecker.Storage.IsStorageService() {
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.Etcd.GetEndpoints(),
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("create etcd client: %w", err)
		}
		defer etcdClient.Close()
		client, err = woodpecker.NewClient(ctx, cfg, etcdClient, false)
		if err != nil {
			return fmt.Errorf("create service client: %w", err)
		}
	} else if cfg.Woodpecker.Storage.IsStorageMinio() {
		client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
		if err != nil {
			return fmt.Errorf("create embedded client: %w", err)
		}
	} else {
		return fmt.Errorf("this example supports storage.type service or minio")
	}
	defer client.Close(context.Background())

	name := fmt.Sprintf("quickstart-%d", time.Now().UnixNano())
	if err := client.CreateLog(ctx, name); err != nil {
		return fmt.Errorf("create log: %w", err)
	}
	handle, err := client.OpenLog(ctx, name)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer handle.Close(context.Background())
	writer, err := handle.OpenLogWriter(ctx)
	if err != nil {
		return fmt.Errorf("open writer: %w", err)
	}
	defer writer.Close(context.Background())

	payload := []byte("hello, Woodpecker")
	result := writer.Write(ctx, &wplog.WriteMessage{Payload: payload})
	if result.Err != nil {
		return fmt.Errorf("write: %w", result.Err)
	}
	fmt.Printf("WROTE log=%s position=%d:%d\n", name, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)

	reader, err := handle.OpenLogReader(ctx, result.LogMessageId, "quickstart")
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	defer reader.Close(context.Background())
	message, err := reader.ReadNext(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if !bytes.Equal(message.Payload, payload) || *message.Id != *result.LogMessageId {
		return fmt.Errorf("read result differs from acknowledged write")
	}
	fmt.Printf("READ position=%d:%d payload=%q\n", message.Id.SegmentId, message.Id.EntryId, message.Payload)
	fmt.Printf("VERIFIED mode=%s messages=1\n", cfg.Woodpecker.Storage.Type)
	return nil
}
