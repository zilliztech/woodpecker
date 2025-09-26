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

package minio

import (
	"io"

	"github.com/minio/minio-go/v7"
)

//go:generate mockery --dir=./common/minio --name=FileReader --structname=FileReader --output=mocks/mocks_minio --filename=mock_file_reader.go --with-expecter=true  --outpkg=mocks_minio
type FileReader interface {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
	Size() (int64, error)
}

type ObjectReader struct {
	*minio.Object
}

func (or *ObjectReader) Size() (int64, error) {
	stat, err := or.Stat()
	if err != nil {
		return -1, err
	}
	return stat.Size, nil
}
