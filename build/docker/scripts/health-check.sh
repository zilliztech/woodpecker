#!/bin/bash
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Default values
GRPC_PORT=${GRPC_PORT:-18080}
GOSSIP_PORT=${GOSSIP_PORT:-17946}

# Check if Woodpecker process is running
if ! pgrep -f "woodpecker" > /dev/null; then
    echo "❌ Woodpecker process not running"
    exit 1
fi

# Check gRPC port (service port)
if ! ss -tuln | grep -q ":$GRPC_PORT "; then
    echo "❌ gRPC port $GRPC_PORT not available"
    exit 1
fi

# Check gossip port (check for any binding, not just localhost)
if ! ss -tuln | grep -q ":$GOSSIP_PORT "; then
    echo "❌ Gossip port $GOSSIP_PORT not available"  
    exit 1
fi

echo "✅ Woodpecker is healthy"
exit 0
