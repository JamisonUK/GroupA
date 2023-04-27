// Add this to the VERY top of the first file loaded in your app
var apm = require('elastic-apm-node').start({

  // Override the service name from package.json
  // Allowed characters: a-z, A-Z, 0-9, -, _, and space
  serviceName: 'GroupA',

  // Use if APM Server requires a secret token
  secretToken: 'dZhhpwxg0BvR6TNEiv',

  // Set the custom APM Server URL (default: http://localhost:8200)
  serverUrl: 'https://39c97bf97e6d4ae08e85f251f3848552.apm.us-central1.gcp.cloud.es.io:443',

  // Set the service environment
  environment: 'my-environment'
})
