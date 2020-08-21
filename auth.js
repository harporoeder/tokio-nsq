var http = require('http');

console.log("starting auth server");

http.createServer(function (req, res) {
    console.log(req.url);
    res.write(JSON.stringify(
        {
          "ttl": 3600,
          "identity": "username",
          "identity_url": "https://test.com",
          "authorizations": [
            {
              "permissions": [
                "subscribe",
                "publish"
              ],
              "topic": ".*",
              "channels": [
                ".*"
              ]
            }
          ]
        }
    ));
    res.end();
}).listen(8080);
