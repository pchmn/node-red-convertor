const elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
  host: 'localhost:9200'
});

client.index({
  index: 'test',
  type: 'docs',
  body: {
    name: "Jacques"
  }
}, function (error, response) {
	if(error)
		console.log("error", error)
	else
		console.log("success", response)
});