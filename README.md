# Dynamic Multiple RTSP Source Management for Savant

## TODOs

- [ ] adapters health checking
- [ ] restart policy
- [ ] containers tracking

## AI detected flaws

The current approach has a few structural problems that will show up quickly once you try to manage more than a couple of RTSP sources.

The state machine is inconsistent.

- [ ] **IN PROGRESS** - You mutate `active_sources` before starting or stopping the Docker container. That means a failed `docker run` leaves a source marked active even though nothing is running, and a failed `docker stop` removes the source from memory while the container may still be alive.

The locking is incomplete.

- [X] The existence checks happen outside the lock, so two concurrent events can race and both think they own the same `id`. If you want this to be safe under load, the check and the mutation need to happen in one critical section.

The process control is fragile.

- [X] `run_adapter()` and `stop_adapter()` ignore return codes, stdout/stderr, and exceptions
- [X] Handle if Docker is missing
- [ ] Handle if the image pull fails
- [X] Handle if the container name is invalid
- [ ] Handle if the RTSP stream is unreachable

The Kafka handling is too brittle for production use.

- [X] `watch_kafka()` at main.py assumes every message has a decodable key and valid JSON value.A malformed message will throw and likely kill the consumer loop.
- [X] There is also no handling for reconnects
- [X] Add commit semantics beyond `enable_auto_commit=True`

The design is not actually dynamic in a robust sense. It can start and stop containers based on Kafka messages, but there is no reconciliation against desired state

- [ ] Add health checking
- [ ] Add restart policy
- [ ] Add tracking of container IDs
- [ ] Add handling for duplicate add/remove events.

There are some smaller code quality issues that will hurt maintainability.

- [X] `id` shadows the built-in name in several functions
- [ ] the Kafka bootstrap host is hardcoded to `localhost:29092`
- [X] the thread startup at main.py does not join or supervise the threads, so failures can disappear silently.

If you want, I can turn this into a concrete redesign plan for a proper multi-RTSP manager with:
1. a single source-of-truth state store,
2. atomic add/remove transitions,
3. a reconciliation loop,
4. and explicit container lifecycle tracking.