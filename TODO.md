### General

- [ ] Take PageManager out of the epoch mechanism
- [ ] Add DropTable command
- [ ] Fix alignment in serialized records
- [ ] Do not crash on shutdown
- [ ] Cache SnapshotDescriptor in server
- [ ] Move SnapshotDescriptor code from CommitManager to TellStore (do not link against CommitManager)
- [ ] Profile and improve scan query evaluation

### Delta-Main Rewrite

- [ ] Truncate update log only up to the point where all entries are sealed

### Log-Structured Memory

- [ ] Elements must be written to the log in version-chain order for later replication
- [ ] Write Revert logs into version chain
