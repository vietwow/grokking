var redis = require('redis').createClient();
var geoip = require('redis-geo')(redis);

var kafka = require('kafka-node');

//es config
var elasticsearch = require('elasticsearch')

// Connect to localhost:9200 and use the default settings
var client = new elasticsearch.Client();

// Connect the client to two nodes, requests will be
// load-balanced between them using round-robin
var client = elasticsearch.Client({
  hosts: [
    'http://61.28.227.202:9200'
  ]
});


var kafkaClient = new kafka.Client('zoo1:2181', 'test-consumer');

/*
var topics = [
     { topic: 'events', partition: 0 }
];
*/

var kafkaConsumer = new kafka.Consumer(kafkaClient, [{topic: 'events'}], {
     autoCommit: false
});


kafkaConsumer.on('message', function (message) {
     //console.log('MESSAGE');
     //console.log(message.value);
     body = JSON.parse(message.value)
     body.timestamp = new Date()
     geoip(body.ip, function (err, info) {
       if (err) throw err;
	body.geoinfo = info
        //console.log(info);
        //console.log(body.geoinfo);
	client.index({
	       index: 'grok',
	       type: body.metric,
	       body: body,
	       refresh: true
	})
     });
});

kafkaConsumer.on('error', function (err) {
     console.log('ERROR');
     console.error(err);
});

kafkaConsumer.on('offsetOutOfRange', function (err) {
     console.log(err);
});
