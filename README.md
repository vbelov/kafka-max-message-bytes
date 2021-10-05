Создаем кластер, топик и пользователей:

```bash
yc kafka cluster create --zone-ids ru-central1-a --brokers-count 1 kafka-max-message-bytes --version 2.6 --resource-preset b2.medium --disk-size 60GB --disk-type network-ssd --network-name default --assign-public-ip
yc kafka topic create --cluster-name kafka-max-message-bytes --partitions 4 --replication-factor 1 --max-message-bytes 10000000 topic1
yc kafka user create producer --cluster-name kafka-max-message-bytes --permission topic=topic1,role=producer --password mysecret
yc kafka user create consumer --cluster-name kafka-max-message-bytes --permission topic=topic1,role=consumer --password mysecret
```

Настраиваем и запускаем тестовое приложение:
```bash
python3.6 -m venv venv
. venv/bin/activate
pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
export KAFKA_BROKERS=<указываем имя хоста брокера в формате hostname:9091>
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem"
python main.py
```

В отдельной вкладке можно запустить consumer:
```bash
python main.py consume
```
