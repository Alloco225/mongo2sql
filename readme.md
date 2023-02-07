# mongo2sql

A simple npm package to convert a MongoDB collection into an SQL script dump that can be executed on a database.

## Installation

```cmd

npm install mongo2sql

```

## Usage

```javascript
const mongo2SQL = require("mongo2sql");

mongo2SQL({
    uri: "mongodb://localhost:27017/database-name",
    collection: "collection-name",
    outputFile: "output-file.sql",
})
    .then(() => console.log("SQL script dump generated successfully"))
    .catch((error) => console.error(error));
```

## Options

| Property   | Type   | Required | Description                                        |
| ---------- | ------ | -------- | -------------------------------------------------- |
| uri        | string | Yes      | The MongoDB connection string                      |
| collection | string | Yes      | The MongoDB collection name to be converted to SQL |
| outputFile | string | Yes      | The file name for the output SQL script            |

## License

This package is licensed under the MIT License.
