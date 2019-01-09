# Rabbit query healer

### Dumpfile to ./var/query_name.log

```bash

go run dump.go 127.0.0.1 5672 login pass /vhost query_name

```

### Restore from to ./var/query_name.log

```bash

go run restore.go 127.0.0.1 5672 login pass /vhost query_name

```