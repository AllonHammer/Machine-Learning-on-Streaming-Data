# Multi Arm Bandits Using Streaming Data

This is a small demonstration of applying the UCB1 algorithm on real time streaming data

## Description

We simulate a stream of winning bids from some sort of real time bidding process. The goal is to 
set a real time reserve price for each auction, and adapting to changes of the bidding distribution.
This distribution is drawn from sample_dist.csv. A Kafka producer generates random
samples from this distribution and a Kafka consumer read those samples in batches, updates the UCB weights
and generates a reserve price.

## Getting Started

### Dependencies

* Apache Kafka server
* Apache Zookeeper
* Python 3.5+



### Executing program

* In main_app.py set the KAFKA_HOST to be the desired host (default localhost)

```
python3 main_app.py
```


## Authors

Allon Hammer