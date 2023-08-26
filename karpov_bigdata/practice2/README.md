# Практика MapReduce

## Подготовка

* создадим кластер
* установим AWS CLI на одной из машин кластера:
  ```shell
  apt install -y awscli
  ```
* скачаем тестовые данные:
  ```shell
  wget -O alice.txt https://www.gutenberg.org/files/11/11-0.txt
  wget -O frank.txt https://www.gutenberg.org/files/84/84-0.txt
  ```
* Создадим папки на HDFS:
  ```shell
  hadoop fs -mkdir /user/root
  hadoop fs -mkdir /user/root/input-data
  hadoop fs -put *.txt input-data/
  ```  
## Разберем исходный код mapper и reducer
## Запустим код
* Скопируем код на кластер
  ```shell
  scp ./*.py root@84.201.188.76:/tmp/mapreduce/ && scp ./run.sh root@84.201.188.76:/tmp/mapreduce/
  ```
* Запустим код
  ```shell
  cd /tmp/mapreduce/
  chmod +x ./run.sh
  ./run.sh 
  ```
* Материалы
  * https://hadoop.apache.org/docs/stable/hadoop-streaming/HadoopStreaming.html
