// Copyright © 2024 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongoutil

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

const (
	defaultMaxPoolSize = 100
	defaultMaxRetry    = 3
)

// buildMongoURI constructs the MongoDB URI from the provided configuration.
func buildMongoURI(config *Config) string {
	credentials := ""
	if config.Username != "" && config.Password != "" {
		credentials = fmt.Sprintf("%s:%s@", config.Username, config.Password)
	}
	tlsStr := "false"
	if config.TLS.EnableTLS {
		tlsStr = "true"
	}

	return fmt.Sprintf("mongodb://%s%s/%s?maxPoolSize=%d&tls=%s", credentials, strings.Join(config.Address, ","), config.Database, config.MaxPoolSize, tlsStr)
}

// shouldRetry determines whether an error should trigger a retry.
func shouldRetry(ctx context.Context, err error) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		if cmdErr, ok := err.(mongo.CommandError); ok {
			return cmdErr.Code != 13 && cmdErr.Code != 18
		}
		return true
	}
}
