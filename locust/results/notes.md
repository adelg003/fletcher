# Locust Testing Notes

## Resources

- OS: Fedora Linux 42 x86_64
- Kernel: Linux 6.15.8-200.fc42.x86_64
- CPU: AMD Ryzen 9 7950X (32) @ 5.88 GHz
- Disk: 2TB NVMe Get 4 SSD (Sabrent)
- Filesystem: btrfs

## Docker Image

- Docker image size: 31.7MB

## Busiest Day Notes

- Fletcher RAM Usage: 28MB
- PostgreSQL RAM Usage: 1.2GB
- Time to run busy season day: 8 minutes 3 seconds
- Errors: None

## Stress Test Notes

### PostgreSQL connections == 10

- Default MAX_CONNECTIONS to 10
  - If not raised, 10 connections to PostgreSQL becomes the limiting factor
  - Each connection uses up to 1 core in PostgreSQL
- Memory: 275MB
  - Looks like request queue in Fletchers memory while waiting for DB to respond
  - If more PG connections are open and pushed to the DB, less request are
    queues in Fletchers memory.
- CPU: 13% of 1 core
- User limit with no failures: N/A
  - Hit 2,500 users and median response time was 10 seconds without hitting any failures
  - Response time should be sub-second
  - Limit is PostgreSQL CPU
- Request per second with no failures: N/A
  - Hit 2,500 users and median response time was 10 seconds without hitting any failures
  - Response time should be sub-second
  - Limit is PostgreSQL CPU
- User limit with performance degradation: 950
- Request per second with no performance degradation: 140

### PostgreSQL connections == 30

- Raise MAX_CONNECTIONS to 30
  - Default is 10 PG Connections
  - If not raised, 10 connections to PostgreSQL becomes the limiting factor
  - Each connection uses up to 1 core in PostgreSQL
  - Next limiting factor, CPU resources for PostgreSQL
- Memory: 160MB
- CPU: 20% of 1 core
- User limit with no failures: N/A
  - Hit 2,500 users and median response time was 8 seconds without hitting any failures
  - Response time should be sub-second
  - Limit is PostgreSQL CPU
- Request per second with no failures: N/A
  - Hit 2,500 users and median response time was 8 seconds without hitting any failures
  - Response time should be sub-second
  - Limit is PostgreSQL CPU
- User limit with performance degradation: 1,300
- Request per second with no performance degradation: 180

## Assessment

Even with more connections and cores, performance increases experience
diminishing returns due to PostgreSQL. Adding more cores and connections does
increase the limit, but not by much. Using raw connections instead of
transactions in PostgreSQL should allow for more performance, but introductions
of data integrity issues while under load are not acceptable.
