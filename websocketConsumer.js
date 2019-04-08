const io = require('socket.io')();

const clients = [];

io.on('connection', (client) => {
  client.on('subscribeToKafkaTopic', (topic) => {
    console.log('client is subscribing to kafka topic ', topic);
    clients.push(client);
  });
});

function callSockets(message) {
  clients.forEach((client) => {
    client.emit('person', message);
  });
}

const kafka = require('kafka-node');
const avroSchemaRregistry = require('avro-schema-registry');

const registry = avroSchemaRregistry('http://localhost:29081');

const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:29092',
});

const consumer = new kafka.Consumer(client, [{ topic: 'people', parition: 0 }], {
  autoCommit: false,
});

async function decodePersonMessage(personMessage) {
  const messageValue = personMessage.value;
  const messageBuffer = Buffer.from(messageValue, 'utf8');
  const decodedPerson = await registry.decode(messageBuffer);

  return decodedPerson;
}

consumer.on('message', async (msg) => {
  const decodedMessage = await decodePersonMessage(msg);
  callSockets(decodedMessage);
  console.log(decodedMessage);
});

consumer.on('error', (error) => {
  console.log('Error during setup of consumer:');

  console.log(error);
});

const port = 8000;
io.listen(port);
console.log('listening on port ', port);
