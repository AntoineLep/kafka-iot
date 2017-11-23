# JAVA

sudo apt-get update -y
sudo apt-get upgrade -y
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer -y


# KAFKA

wget http://apache.crihan.fr/dist/kafka/1.0.0/kafka_2.11-1.0.0.tgz
sudo mkdir /opt/Kafka
sudo tar -xvf kafka_2.11-1.0.0.tgz -C /opt/Kafka/
sudo /opt/Kafka/kafka_2.11-1.0.0/bin/kafka-server-start.sh /opt/Kafka/kafka_2.11-1.0.0/config/server.properties


# LIBRDKAFKA

cd /opt
git clone https://github.com/edenhill/librdkafka.git
./configure
make
sudo make install



# PYTHON REQUIREMENTS
# Install packages in the requirements.txt file
