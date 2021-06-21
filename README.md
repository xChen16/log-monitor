# log-monitor

->log->influxdb->grafana

## influxdb
```
docker pull influxdb:2.0.7
docker run -d -p 8086:8086 --name my_influxdb influxdb
```
## grafana
```
docker pull grafana
docker run -d -p 3000:3000 grafana/grafana
default account/passwword is `admin/admin`
```
