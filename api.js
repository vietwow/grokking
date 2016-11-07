var bodyParser = require('body-parser');
var express = require('express');
var kafka = require('kafka-node');
// Use Zookeeper connect String in Client:
var client = new kafka.Client('10.60.3.132:2181','test-client');
var producer = new kafka.Producer(client);
producer.addListener('ready', function () {
    console.log('Kafka producer is ready');
});

app = express();
app.use(bodyParser.json());
/*app.use(bodyParser.urlencoded({
  extended: true
}));
*/

app.post('/logs/', function(req, res) {
    //console.log(req.body);
    if(producer) {
        payload = [
            { topic: 'logs', messages: JSON.stringify(req.body)}
        ];
        producer.send(payload, function (err, data) {
            if (err) {
                res.send(500, err);
            } else {
                res.send(200, 'Message is queued...');
            }
        });
        
    } else {
        res.send(500, 'Producer is not initialized');
    }
});

app.listen(8083);
