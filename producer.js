const fs = require('fs');
const kafka = require('kafka-node');
const faker = require('faker');

const avroSchemaRregistry = require('avro-schema-registry');

const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:29092',
});

const registry = avroSchemaRregistry('http://localhost:29081');

const schemaFileData = fs.readFileSync('peopleSchema.json', 'utf8');
const schema = JSON.parse(schemaFileData);

async function encodePersonBySchema(person) {
  console.log('encoding person by schema...');

  const encodedPerson = await registry.encodeMessage('people', schema, person);

  console.log('done encoding.');

  return encodedPerson;
}

const producer = new kafka.Producer(client);

async function startSending() {
  const fakePerson = {
    firstName: faker.name.firstName(),
    lastName: faker.name.lastName(),
    birthDate: faker.date.past().toLocaleDateString(),
  };

  const payload = await encodePersonBySchema(fakePerson);

  const payloads = [
    {
      topic: 'people',
      messages: [payload],
      partition: 0,
    },
  ];
  producer.send(payloads, (err, data) => {
    if (err) {
      console.log('ERROR');
      console.log(err);
    } else {
      console.log(data);
    }
  });
}

producer.on('ready', () => {
  console.log('Starting the sending...');

  setInterval(startSending, 2000);
});

producer.on('error', (error) => {
  console.log('Error during setup of producer:');

  console.log(error);
});
