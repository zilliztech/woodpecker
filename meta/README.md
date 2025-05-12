### Meta in Etcd
- `root`: the prefix used for prefixing all the keys used for storing the metadata. The `root` default value is `woodpecker`.
- `root`/logs: key for storing log instances.
- `root`/logs/`<log_name>`: key for storing unbounded log stream instance meta.
- `root`/logs/`<log_name>`/segments: key for storing segments of the log instance.
- `root`/logs/`<log_name>`/segments/`<segment_id>`: key for id a segment of the log.
- `root`/quorums: the prefix used for prefixing all the quorum keys.
- `root`/quorums/`<quorum_id>`: key for quorum information.
- `root`/quorums/idgen: key for generating quorum id.
- `root`/logstores: the prefix used for storing LogStores instances.
- `root`/logstores/`<logstore_id>`: key for registering LogStore.
- `root`/instance: key for current cluster instance.
- `root`/logidgen: key for logId Generator.
- `root`/quorumidgen: key for quorumId Generator.
- `root`/readers/`<log_id>`/`<reader_unique_name>`: key for opened readers temporary state info.
- `root`/cleaning/`<log_id>`/`<segment_id>`: key for segments cleaning intermediate state info.

#### Registration Manager

- LogStore Register: write to key "`root`/logstores/`<logstore_id>`" with a keepalive lease.
- LogStore Unregister: delete key "`root`/logstores/`<logstore_id>`".

#### Registration Client

- Get available LogStores: range operation to fetch keys between "`root`/logstores/" and "`root`/logstores_end/".
- Watch available LogStores: watch operation to watch keys between "`root`/logstores/" and "`root`/logstores_end/".

#### Id Generator

- Id generation: get key, increment by 1 and then update with a txn operation.

#### Segment Manager

- Create Segment: a txn operation to put Segment metadata to key "`root`/logs/`<log_name>`/segments/`<segment_id>`" when this key doesn't exist
- Write Segment: a txn operation to put Segment metadata to key "`root`/logs/`<log_name>`/segments/`<segment_id>`" when key version matches
- Delete Segment: a delete operation on key "`root`/logs/`<log_name>`/segments/`<segment_id>`"
- Read segment: a get operation on key "`root`/logs/`<log_name>`/segments/`<segment_id>`"
- Iteration: a range operation fetch keys between "`root`/logs/`<log_name>`/segments/" and "`root`/logs/`<log_name>`/segments_end/"

#### Quorum Manager

- Create QuorumInfo: a txn operation to put Quorum Info to key "`root`/quorums/`<quorum_id>`" when this key doesn't exist
- Write QuorumInfo: a txn operation to put Quorum Info to key "`root`/quorums/`<quorum_id>`" when key version matches
- Delete QuorumInfo: a delete operation on key "`root`/quorums/`<quorum_id>`"
- Read QuorumInfo: a get operation on key "`root`/quorums/`<quorum_id>`" 

#### Status information

##### Reader Temporary Information
- Create Reader Temp Info: a put operation with lease to key "`root`/readers/`<log_id>`/`<reader_unique_name>`" containing reader's position
- Update Reader Temp Info: a put operation to key "`root`/readers/`<log_id>`/`<reader_unique_name>`" to update reader's current position
- Get Reader Temp Info: a get operation on key "`root`/readers/`<log_id>`/`<reader_unique_name>`"
- List All Readers for Log: a range operation to fetch keys between "`root`/readers/`<log_id>`/" and "`root`/readers/`<log_id>`_end/"
- Delete Reader Temp Info: a delete operation on key "`root`/readers/`<log_id>`/`<reader_unique_name>`" when reader closes

##### Segment Cleanup Status Information
- Create Cleanup Status: a txn operation to put cleanup status to key "`root`/cleaning/`<log_id>`/`<segment_id>`" when this key doesn't exist
- Update Cleanup Status: a txn operation to put cleanup status to key "`root`/cleaning/`<log_id>`/`<segment_id>`" when key exists
- Get Cleanup Status: a get operation on key "`root`/cleaning/`<log_id>`/`<segment_id>`"
- List All Cleanup Statuses: a range operation to fetch keys between "`root`/cleaning/`<log_id>`/" and "`root`/cleaning/`<log_id>`_end/"
- Delete Cleanup Status: a delete operation on key "`root`/cleaning/`<log_id>`/`<segment_id>`" when cleanup completes or fails

