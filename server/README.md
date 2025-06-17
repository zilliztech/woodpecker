# LogStore related components.

This is the location for LogStore Server related classes. It is to prepare for separating common, protocol, client and
server modules.

## LogStore ObjectStorage layout

layout: `root`/`<instance_id>`/`<log_id>`/`<segment_id>`/`<fregement_id>`

- `root`: the prefix used for prefixing all the log data. The `root` default value is `woodpecker`.
- `instance_id`: identifier for the current woodpecker cluster instance.
- `log_id`: identifier for the log.
- `segment_id`: identifier for the segment of the log.
- `<fregement_id>`: identifier for the object of the logfile, which represents a fragment of the logfile.
