const kafka = require('kafka-node');

const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:29092',
});

const consumer = new kafka.Consumer(client, [{ topic: 'people', parition: 0 }], {
  autoCommit: false
});

consumer.on('message', (msg) => {
  console.log(msg);
});

consumer.on('error', (error) => {
  console.log("Error during setup of consumer:");

  console.log(error);
})