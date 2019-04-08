const fs = require('fs');
const kafka = require('kafka-node');
const avroSchemaRregistry = require('avro-schema-registry');

const registry = avroSchemaRregistry('http://localhost:29081');
const schemaFileData = fs.readFileSync('peopleSchema.json', 'utf8');
const schema = JSON.parse(schemaFileData);

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
  console.log(decodedMessage);
});

consumer.on('error', (error) => {
  console.log('Error during setup of consumer:');

  console.log(error);
});
