# RDMA Test Benchmark

This is a benchmark for testing RDMA network performance.
- rc_pingpong: used to test the send and receive of rdma requests in the form of pingpong.
- rc_send: used to test the situation where one end continuously post send requests to the other end.
- rc_send_sched: Create a latency-sensitive thread to send small messages, and create a bandwidth-sensitive thread to send large messages. You can choose whether to enable the scheduler we provide.
- criu/basement: Used to test scenarios for migrating containers using CRIU.

## How to build
Follow the steps below:
```shell
git clone git@github.com:Idealist226/test_for_rdma.git && cd test_for_rdma
mkdir build && cd build
cmake ..
make
```