var kafka = require('kafka-node');

//pg config
var pg = require('pg');
var conString = 'postgres://postgres:newhacker@61.28.227.196/grok';

var kafkaClient = new kafka.Client('10.60.3.132:2181', 'test-consumer');

var topics = [
     { topic: 'logs', partition: 0 }
];

var kafkaConsumer = new kafka.Consumer(kafkaClient, topics, {
     autoCommit: false
});

kafkaConsumer.on('message', function (message) {
     //console.log('MESSAGE');
     //console.log(message);

     console.log(JSON.parse(message.value));
     pg.connect(conString, function(err, client, done) {
       if (err) {
         return console.error('error fetching client from pool', err);
       }
       console.log("connected to database");
       client.query('INSERT INTO logs.events_write(ts, body) VALUES(CURRENT_TIMESTAMP, $1)', [JSON.parse(message.value)], function(err, result) {
         done(); //this done callback signals the pg driver that the connection can be closed or returned to the connection pool
         if (err) {
           return console.error('error running query', err);
         }
         console.log(result);
       });
     });
});

kafkaConsumer.on('error', function (err) {
     console.log('ERROR');
     console.error(err);
});

kafkaConsumer.on('offsetOutOfRange', function (err) {
     console.log(err);
});
