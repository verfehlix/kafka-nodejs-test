const kafka = require('kafka-node');
const faker = require('faker');

const client = new kafka.KafkaClient({
  kafkaHost: 'localhost:29092'
});

const producer = new kafka.Producer(client);

function startSending() {
  const fakePerson = {
    firstName: faker.name.firstName(),
    lastName: faker.name.lastName(),
    birthDate: faker.date.past(),
  };

  const payloads = [
    {
      topic: 'people',
      messages: [JSON.stringify(fakePerson)],
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

// test
producer.on('ready', () => {
  console.log('Starting the sending...');

  setInterval(startSending, 2000);
});

producer.on('error', (error) => {
  console.log("Error during setup of producer:");

  console.log(error);
})