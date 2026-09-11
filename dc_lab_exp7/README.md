# DC Lab — Experiment 7: Eventual Consistency via Gossip Replication + LWW

This folder implements a minimal replicated key-value store in Python using gRPC.

## Files

- `proto/replica.proto` — contract for `SaveValue`, `SyncUpdate`, and `GetValue`
- `replica_node.py` — each replica accepts writes locally and gossips updates asynchronously
- `replica_client.py` — two concurrent writers hit different replicas to create a conflict
- `test_eventual_consistency.py` — unit tests for Lamport ordering and Last-Write-Wins

## Run it

1. Activate the project venv.
2. Generate gRPC stubs:

   ```powershell
   .\.venv\Scripts\python.exe -m grpc_tools.protoc -I=dc_lab_exp7/proto --python_out=dc_lab_exp7 --grpc_python_out=dc_lab_exp7 dc_lab_exp7/proto/replica.proto
   ```

3. Start the replicas in separate terminals:

   ```powershell
   python dc_lab_exp7/replica_node.py A 60301
   python dc_lab_exp7/replica_node.py B 60302
   python dc_lab_exp7/replica_node.py C 60303
   ```

4. Run the client once all replicas are listening:

   ```powershell
   python dc_lab_exp7/replica_client.py
   ```

The client intentionally writes to two different replicas at nearly the same time. Immediately afterward, replicas may disagree; after the gossip window they must converge on the logically later write according to Lamport timestamps and the `(timestamp, origin)` tiebreak rule.
