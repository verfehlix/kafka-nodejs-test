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

const fakePerson = {
  firstName: faker.name.firstName(),
  lastName: faker.name.lastName(),
  birthDate: faker.date.past().toLocaleDateString(),
};

registry
  .encodeMessage('topic', schema, fakePerson)
  .then((msg) => {
    console.log(msg); // <Buffer 00 00 00 00 01 18 74 65 73 74 20 6d 65 73 73 61 67 65>

    return registry.decode(msg);
  })
  .then((msg) => {
    console.log(msg); // test message
  });

// const producer = new kafka.Producer(client);

// function startSending() {
//   const fakePerson = {
//     firstName: faker.name.firstName(),
//     lastName: faker.name.lastName(),
//     birthDate: faker.date.past(),
//   };

//   const payloads = [
//     {
//       topic: 'people',
//       messages: [JSON.stringify(fakePerson)],
//       partition: 0,
//     },
//   ];
//   producer.send(payloads, (err, data) => {
//     if (err) {
//       console.log('ERROR');
//       console.log(err);
//     } else {
//       console.log(data);
//     }
//   });
// }

// producer.on('ready', () => {
//   console.log('Starting the sending...');

//   setInterval(startSending, 2000);
// });

// producer.on('error', (error) => {
//   console.log('Error during setup of producer:');

//   console.log(error);
// });
