import fs from "fs";
import readline from "readline";
import path from "path";
import chalk from "chalk";
//

// Sql formatter
import { format } from "sql-formatter";
// Sql driver
import mysql from "mysql";

let sqlConfig = {
    // user: process.env.DB_USER,
    // password: process.env.DB_PWD,
    // database: process.env.DB_NAME,
    user: "root",
    password: "",
    database: "test_converter_db", // TODO make dynamic
    // server: "localhost",
    host: "localhost",
    // port: 61427, // make sure to change port
    port: 3306, // make sure to change port
    // pool: {
    //     max: 10,
    //     min: 0,
    //     idleTimeoutMillis: 30000,
    // },
    // options: {
    //     encrypt: true, // for azure
    //     trustServerCertificate: false, // change to true for local dev / self-signed certs
    // },
};

const primitiveTypes = ["string", "number", "bigint", "boolean", "undefined", "symbol", "null"];

// TODO add global debug

const appConfig = {
    debug: true,
    verbose: false,
    backTick: true,
    querySeparator: "--<**/endquery**>", // ! IMPORTANT always add a \n after the querySeparator
    primaryKeyName: "primary_id", // ? usually id but mwell
    foreignKeySuffix: "_primary_id", // usually _id
};

const tableSettings = {
    exception: { startsWith: "cred_pub_tb_" },
};

let sqlRequestObject;

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

const _exit = (returnValue = 1) => {
    process.exit(returnValue);
};

const _confirmOrInput = async (text = "", defaultValue = null, format = true) =>
    new Promise((resolve, reject) => {
        try {
            return rl.question(`${text}: (${defaultValue}) `, (answer) => {
                answer = answer.toLowerCase();
                let result = defaultValue;
                if (answer !== "\n" && answer !== "\r" && answer !== "\r\n") {
                    // TODO ask for ${text}
                    // process.exit();
                    if (answer) result = answer;
                }
                // TODO add name confirm
                // if (format) result = _slugify(result); // TODO add slugify checker
                _Logger.success(`${text} "${result}"`);
                resolve(result);
            });
        } catch (error) {
            _Logger.error(error);
            reject(error);
            _exit();
        }
    });
const _confirmAction = async (text = "", defaultValue = false) =>
    new Promise((resolve, reject) => {
        try {
            let q = defaultValue ? "Y/n" : "y/N";
            return rl.question(`${text} ? (${q}) `, (answer) => {
                answer = answer.toLowerCase();
                if (defaultValue) {
                    if (answer === "n") {
                        resolve(false);
                    }
                    resolve(true);
                } else {
                    if (answer === "y") {
                        resolve(true);
                    }
                    resolve(false);
                }
            });
        } catch (error) {
            _Logger.error(error);
            reject(error);
            _exit();
        }
    });

const _readFile = (filePath) => {
    _Logger.log("reading file ", filePath, " ...");
    try {
        let data = fs.readFileSync(filePath, "utf8");
        return data;
    } catch (err) {
        _Logger.error("Cannot read file : ", filePath);
        _Logger.warning("Please make sure the .json file exists and that the path is correct");
        _exit();
    }
};

const _doesDBExists = (databaseName) =>
    new Promise((resolve, reject) => {
        return _executeSQLScript("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + databaseName + "'", (error, results, fields) => {
            if (error) {
                resolve(false);
            }
            if (results.length == 0) {
                resolve(false);
            }
            resolve(true);
        });
    });
const _createDatabase = (databaseName) =>
    new Promise((resolve, reject) => {
        return _executeSQLScript("CREATE DATABASE IF NOT EXISTS " + databaseName + ";", (error, results, fields) => {
            if (error) {
                resolve(false);
            }
            resolve(true);
        });
    });

const _parseData = (data) => {
    _Logger.log("parsing data ...");
    try {
        let jsonData = JSON.parse(data);
        return jsonData;
    } catch (err) {
        _Logger.error("Error parsing data : ", err);
        _Logger.warning("Please make sure the data is well formatted");
        _exit();
    }
};
const _slugify = (str, separator = "_") => {
    return str
        .toString()
        .normalize("NFD") // split an accented letter in the base letter and the acent
        .replace(/[\u0300-\u036f]/g, "") // remove all previously split accents
        .toLowerCase()
        .trim()
        .replace(/[^a-z0-9 ]/g, "") // remove all chars not letters, numbers and spaces (to be replaced)
        .replace(/\s+/g, separator);
};
const _generateSQLFieldType = (type, exempleValue = null) => {
    _Logger.log("Get sql type of ", type, exempleValue);
    const sqlTypesMap = {
        int: "INT",
        bigint: "BIGINT",
        varchar: `VARCHAR(255)`,
        text: `TEXT`,
        blob: `BLOB`,
        decimal: "DECIMAL(11, 8)",
        char: `CHAR(255)`,
        boolean: "TINYINT(1)",
        date: "DATE",
        json: "JSON",
    };
    let sqlType = "varchar"; // Default type that works with most databases
    // Main types available
    // String
    // Number
    // Bigint
    // Boolean
    // Undefined
    // Null
    // Symbol
    // Object
    switch (type) {
        case "string":
            // TODO check for length
            // sqlType = "varchar";
            // if(exempleValue){
            //     if(exempleValue.length >= 65535){
            //         sqlType = 'text';
            //     }
            // }
            sqlType = "text"; // TODO fix ER_TOO_BIG_ROWSIZE
            break;
        case "number":
            // Verify wether the number is an integer or a double
            if (exempleValue) {
                sqlType = "int";
                if (Math.abs(exempleValue) >= 2147483647) sqlType = "bigint";
                if (exempleValue % 1 !== 0) {
                    sqlType = "decimal";
                    if (`${exempleValue}`.length >= 11) {
                        sqlType = "varchar";
                    }
                }
                if (exempleValue == 0) sqlType = "varchar";
            }
            sqlType = "varchar"; // ! IMPORTANT make numbers varchar because otherwise there's just too many problems
            break;
        case "boolean":
            sqlType = "boolean";
            break;
        default: // Or varchar
            // sqlType = "json";
            sqlType = "text";
            break;
    }
    let result = sqlTypesMap[sqlType];
    if (appConfig.debug) _Logger.info("SQL type :", result);
    return result;
};

const _escapeString = function (str, key = null) {
    let r = str;
    try {
        r = r.replace(/\'/g, "\\'");
    } catch (error) {
        _Logger.error("_escapeString remove ' character ", r, "key:", key);
    }
    try {
        r = r.replace(/ /g, " ");
    } catch (error) {
        _Logger.error("_escapeString replace non breaking space ", r, "key:", key);
    }
    return r;
};
const _escapeSpace = function (str, key = null) {
    let r = str;
    try {
        r = r.replace(/ /g, " ");
    } catch (error) {
        _Logger.error("_escapeSpace replace non breaking space ", r, "key:", key);
    }
    return r;
};

// This function flattens objects to their last depths,
// smushing the attribute names
// doesn't flatten arrays
function _flattenObject__(obj) {
    _Logger.log("_flattenObject");

    var toReturn = {};

    for (var i in obj) {
        if (!obj.hasOwnProperty(i)) continue;

        if (typeof obj[i] == "object" && !Array.isArray(obj[i])) {
            var flatObject = _flattenObject(obj[i]);
            for (var x in flatObject) {
                if (!flatObject.hasOwnProperty(x)) continue;

                toReturn[i + "." + x] = flatObject[x];
            }
        } else {
            toReturn[i] = obj[i];
        }
    }
    return toReturn;
}
function _flattenObject(obj) {
    _Logger.log("_flattenObject");

    var toReturn = {};

    for (var i in obj) {
        if (!obj.hasOwnProperty(i)) continue;

        if (typeof obj[i] == "object" && !Array.isArray(obj[i])) {
            let startsWith = "cred_pub_tb_";

            const regex = new RegExp(`^(?!${startsWith})`);
            if (regex.test(i)) {
                var flatObject = _flattenObject(obj[i]);
                for (var x in flatObject) {
                    if (!flatObject.hasOwnProperty(x)) continue;
                    //
                    toReturn[i + "." + x] = flatObject[x];
                }
            } else {
                toReturn[i] = obj[i];
            }
        } else {
            toReturn[i] = obj[i];
        }
    }
    return toReturn;
}

//
const _generateTableScript = (table) => {
    //
    // ! VERY_IMPORTANT always add ${appConfig.querySeparator} after ';'
    // ? It serves a query separator (duh!) to separate queries in final mysql run
    _Logger.log("Generating table script for", table.name);

    // TODO add dbname.tbname if strict
    // * Table creation script

    let result = `
        --
        -- Table structure for table ${_backTick()}${table.name}${_backTick()}
        --

        DROP TABLE IF EXISTS ${_backTick()}${table.name}${_backTick()}; ${appConfig.querySeparator} \n\n

        CREATE TABLE ${_backTick()}${table.name}${_backTick()} (\n`;
    // Add id if field id doesn't exist

    if (!table.fields.find((f) => f.name == appConfig.primaryKeyName)) {
        result += `${_backTick()}${appConfig.primaryKeyName}${_backTick()} bigint UNSIGNED NOT NULL AUTO_INCREMENT`;

        // Add first comma in fields list after id
        if (table.fields.length > 0) {
            result += ",\n";
        }
    }

    let fieldValues = [];
    let valuesMaxLength = 0;

    // Add fields from fields list
    for (let i = 0; i < table.fields.length; i++) {
        let field = table.fields[i];

        let attributes = "";

        let nullable = field.nullable ?? field.name != appConfig.primaryKeyName;

        // if (field.comment) {
        //     attributes += "COMMENT '" + field.comment + "'";
        // }
        if (field.name == appConfig.primaryKeyName) {
            // if(field.type !=)
        }

        let fieldSQL = `${_backTick()}${field.name}${_backTick()} ${field.type} ${attributes}`;
        // Add null by default
        if (nullable) {
            fieldSQL += " DEFAULT NULL";
        }
        if (i < table.fields.length - 1) {
            // ?- Add comma only if not at the en of the fields list
            fieldSQL += ",\n";
        }

        fieldValues.push({
            name: field.name,
            values: field.values,
        });

        if (field.values.length > valuesMaxLength) {
            valuesMaxLength = field.values.length;
        }
        // Add row line to table creation
        result += fieldSQL;
    }
    // Set up primary key
    result += ",\n" + "PRIMARY KEY (`" + appConfig.primaryKeyName + "`)";
    // ?- Add final line
    result += "\n)\n ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;" + appConfig.querySeparator + " \n";

    // * Table data insertion script

    let tableInsertSQL = "";
    _Logger.info("Generated table creation script for", table.name);
    _Logger.log("Generating table insert values script for", table.name);
    if (valuesMaxLength > 0) {
        // if (false) {
        // ? Should I generate ids automatically

        tableInsertSQL = ` \n\nINSERT INTO ${_backTick()}${table.name}${_backTick()} (`;

        if (false) {
            // include id
            tableInsertSQL += "`" + appConfig.primaryKeyName + "`";
        }

        // for each field adding field name for insert statement
        for (let i = 0; i < table.fields.length; i++) {
            // for(const fieldName in table.fields ){
            let field = table.fields[i];
            // Add the field name if it's values are empty ?
            tableInsertSQL += `${_backTick()}${field.name}${_backTick()}`;
            if (i < table.fields.length - 1) {
                // Add trailing comma
                tableInsertSQL += ", ";
            }
        }
        // close the attributes list
        tableInsertSQL += ") VALUES \n";

        // loop thru values

        for (let j = 0; j < valuesMaxLength; j++) {
            // check for each field value now
            tableInsertSQL += "(";
            let valuesLength = valuesMaxLength;

            for (let i = 0; i < table.fields.length; i++) {
                let field = table.fields[i];
                valuesLength = field.values.length;
                // tableInsertSQL += `${_backTick()}${field.name}\``;
                let valueToInsert = "NULL";
                let value = field.values[j];

                // Check that all the values are the same
                // field.values.every(v => )
                if (value) {
                    if (field.originalType == "string") {
                        if (typeof value != "string") {
                            // ? In case there's disorder (like there always is with mongodb) when having multiples values
                            // ? Convert to string
                            value = value.toString();
                        }
                        // escape string and insert
                        valueToInsert = `'${_escapeString(value, field.name)}'`;
                    } else {
                        //
                        // valueToInsert = value;
                        valueToInsert = value;
                    }
                }

                tableInsertSQL += valueToInsert;

                // if (i < valuesLength - 1) {
                if (i < table.fields.length - 1) {
                    // Add trailing comma
                    tableInsertSQL += ", ";
                }
                //
            }
            tableInsertSQL += ")";

            if (j < valuesMaxLength - 1) {
                // Add trailing comma
                tableInsertSQL += ",\n";
            }
        }
        tableInsertSQL += ";\n\n" + appConfig.querySeparator + "\n";
        // for each
    }
    // Add code for insertion
    result += tableInsertSQL;

    _Logger.success("Generated table script for", table.name);
    return result;
};

let _addBackticks = true;
const _backTick = () => (_addBackticks ? "`" : "");
const _generateSQLScript = (sqlRequestObject) => {
    //
    _Logger.warning("Generating SQL Script");

    let SQLQuery = "";
    SQLQuery = `
        -- Author : AMANE HOSANNA
        -- 
        -- Database: ${_backTick()}${sqlRequestObject.databaseName}${_backTick()}
        --
        -- Generated on ${new Date().toGMTString()}
        -- --------------------------------------------------------`;

    // uncomment if you wan to create the database (which is already taken care of)
    if (false) {
        SQLQuery += `
            CREATE DATABASE IF NOT EXISTS ${_backTick()}${sqlRequestObject.databaseName}${_backTick()}
        `;
    }

    let tablesScripts = "";
    for (var i = 0; i < sqlRequestObject.tables.length; i++) {
        let table = sqlRequestObject.tables[i];
        tablesScripts = tablesScripts + "\n" + _generateTableScript(table);
    }

    SQLQuery += "\n " + tablesScripts;
    // Format the sql query
    try {
        SQLQuery = format(SQLQuery);
    } catch (error) {
        _Logger.error(" Error formatting SQL CODE");
        // _Logger.error(error);
    }

    _Logger.info("Generated SQL Script, writing into output.sql");
    fs.writeFileSync("output.sql", SQLQuery);
    _Logger.success("Generated SQL Script at output.sql");

    return SQLQuery;
};

const _executeSQLScript = async (sqlQuery, callback = null) => {
    // _Logger.warning("Executing SQL Script");

    try {
        // make sure that any items are correctly URL encoded in the connection string
        let connection = mysql.createConnection(sqlConfig);
        connection.connect();

        // Here you have to run the queries separately of the mysql with throw an error
        let queryLines = sqlQuery.split(appConfig.querySeparator);
        for (let i = 0; i < queryLines.length; i++) {
            let query = queryLines[i];
            if (query == "") {
                _Logger.info("Skipped Empty Query ", i + 1, "/", queryLines.length);
                continue;
            }
            _Logger.log("Executing query ", i + 1, "/", queryLines.length);
            connection.query(
                query,
                callback ??
                    function (error, results, fields) {
                        // if (error) throw error;
                        if (error) {
                            _Logger.error("Executing query ", i + 1, "/", queryLines.length, query.substr(0, 50));
                            _Logger.error(error);
                        }
                        _Logger.success("Executed query ", i + 1, "/", queryLines.length);
                    }
            );
        }
        connection.end();
    } catch (err) {
        _Logger.error("Could not execute script");
        _Logger.error(err);
        _exit();
    }
};

const _Logger = {
    log: (...args) => console.log(chalk.dim(">> ", ...args)),
    info: (...args) => console.log(chalk.blue(":: ", ...args)),
    success: (...args) => console.log(chalk.green("<< ", ...args)),
    error: (...args) => console.log(chalk.red("xx ", ...args)),
    warning: (...args) => console.log(chalk.yellow("? ", ...args)),
};

const _generateSQLObjectTablesFromCollection = ({ collectionName = null, collectionData = null, relationship = {}, level = 0 } = {}) => {
    _Logger.warning("Generating SQL Object from Collection : ", collectionName, "depth : ", level, "parent :", JSON.stringify(relationship));
    if (!collectionName) {
        _Logger.error("collectionName not defined");
        _Logger.warning("Provide collectionName");
        return;
    }
    let collection = [];

    let depth = level;

    collection = Array.isArray(collectionData) ? collectionData : [collectionData];
    // TODO add validation
    if (collection.length == 0) {
        _Logger.error("collectionName ", collectionName, " is empty");
        _Logger.warning("The table ", collectionName, " will not be created");
        return;
    }
    _Logger.info("Generating Relational Database Model");

    let tableIndex = sqlRequestObject.tables.findIndex((t) => t.name == collectionName);

    if (tableIndex === -1) {
        // Create table
        sqlRequestObject.tables.push({
            name: collectionName,
            fields: [],
        });
        tableIndex = sqlRequestObject.tables.findIndex((t) => t.name == collectionName);
    }
    // Manage foreign keys
    let { belongsTo, foreignKey, foreignKeyValue } = relationship;
    let hasForeignKey = belongsTo && foreignKey && foreignKeyValue;

    for (let i = 0; i < collection.length; i++) {
        // for each document
        let document = collection[i];
        // ? Flatten object
        // Flatten tables object attributes
        document = _flattenObject(document); // Remove this line to not flatten the object

        // Add foreign key if exists
        if (hasForeignKey) {
            // Check if foreign key field already exists
            let foreignKeyIndex = sqlRequestObject.tables[tableIndex].fields.findIndex((f) => f.name == foreignKey);
            if (foreignKeyIndex !== -1) {
                // if foreign key exists, just add value
                let field = sqlRequestObject.tables[tableIndex].fields[foreignKeyIndex];
                let values = [...field.values, foreignKeyValue];
                // update field
                sqlRequestObject.tables[tableIndex].fields[foreignKeyIndex].values = values;
            } else {
                // if foreign key doesn't exist, create it
                let field = {
                    name: foreignKey,
                    type: _generateSQLFieldType(typeof foreignKeyValue, foreignKeyValue),
                    originalType: typeof foreignKeyValue,
                    values: [foreignKeyValue],
                };
                // add field to table's fields
                sqlRequestObject.tables[tableIndex].fields = [...sqlRequestObject.tables[tableIndex].fields, field];
            }
        }
        // for each key in document
        _Logger.warning("Parsing document ", i, "/", collection.length);
        for (const documentKey in document) {
            _Logger.log("Parsing document key", JSON.stringify(documentKey));
            //
            let value = document[documentKey];
            let type = typeof value;

            if (Array.isArray(value)) {
                type = "array";
            }
            // sanitize value if string containing nonbreakeable space
            if (type == "string") {
                value = _escapeSpace(value);
            }

            // * FIELD FOR PRIMITIVE VALUE
            if (primitiveTypes.includes(type)) {
                // add directly to sql
                // check if document key exists
                let fieldIndex = sqlRequestObject.tables[tableIndex].fields.findIndex((f) => f.name == documentKey);
                // if field exists
                if (fieldIndex !== -1) {
                    // update field with value
                    let field = sqlRequestObject.tables[tableIndex].fields[fieldIndex];
                    let values = [...field.values, value];
                    // update field
                    sqlRequestObject.tables[tableIndex].fields[fieldIndex].values = values;
                } else {
                    // if field doesn't exist create it
                    let field = {
                        name: documentKey,
                        type: _generateSQLFieldType(type, value),
                        originalType: type,
                        values: [value],
                    };
                    // add field to table's fields
                    sqlRequestObject.tables[tableIndex].fields = [...sqlRequestObject.tables[tableIndex].fields, field];
                }
                _Logger.info("Added primitive field", type, documentKey);
            }

            // * FIELD FOR OBJECT VALUE
            if (type === "object") {
                // Smush attributes except exceptions
                let exception = { startsWith: "cred_pub_tb_" };
                if (exception && exception.startsWith) {
                    const regex = new RegExp(`^(?!${exception.startsWith})`);
                    // Only allow fields that don't start with exception.startsWith
                    // if(!regex.test(valueKey)){
                    if (!regex.test(documentKey)) {
                        _Logger.log("Exception cred_pubs", documentKey);
                        // Create a new table
                        _generateSQLObjectTablesFromCollection({
                            collectionName: documentKey,
                            collectionData: value,
                            relationship: {
                                belongsTo: collectionName,
                                foreignKey: collectionName + appConfig.foreignKeySuffix,
                                foreignKeyValue: i + 1,
                            },
                        });
                    }
                    // if key doesn't start with cred_pub_ then add every single child as a key
                }
            }
            // * FIELD FOR ARRAY VALUE
            if (type === "array") {
                // * FIELD FOR OBJECT VALUE
                // Check that all children are objects and not arrays // ! they must be in order for this array to be considered a table

                if (value.every((v) => typeof v === "object" && !Array.isArray(v))) {
                    // Create a new table

                    let tableName = documentKey;

                    // if(depth >= 1){
                    // let previousNames = collectionName.split(".");
                    // let parentName = previousNames[previousNames.length-1];
                    let parentName = collectionName;
                    tableName = parentName + "." + tableName;
                    //

                    // }
                    _Logger.info("TABLE ", tableName, "depth:", depth, "parentName", collectionName);
                    // take the latest part of the name then add the suffix if depth > 1

                    _generateSQLObjectTablesFromCollection({
                        collectionName: tableName,
                        collectionData: value,
                        relationship: {
                            belongsTo: collectionName,
                            foreignKey: collectionName + "_id",
                            foreignKeyValue: i + 1,
                        },
                        level: depth + 1,
                    });
                } else {
                    // if all children are
                }
            }
            // * FIELD FOR ARRAY VALUE
        }
        _Logger.success("Parsed document ", i, "/", collection.length);
    }
    _Logger.success("Generated SQL Object from Collection : ", collectionName);

    return;
};

async function main() {
    const filePath = process.argv[2];
    const fileName = path.basename(filePath);

    // console.log(`The file name is: ${fileName}`);
    // console.log(`The file path is: ${filePath}`);
    let collectionName = fileName.replace(/.json/g, "");
    let databaseName = "db_test";

    //
    let data = _readFile(filePath);
    let jsonData = _parseData(data);

    // Get the collection name if the file is readable

    let dbExists = false;
    do {
        databaseName = await _confirmOrInput("Database name", databaseName);
        
        dbExists = await _doesDBExists(databaseName);
        if (!dbExists) {
            _Logger.error("Database ", databaseName, " doesn't exist");

            let shouldCreateDB = await _confirmAction("Create database '" + databaseName + "'");

            if (shouldCreateDB) {
                let dbCreated = await _createDatabase(databaseName);
                if (dbCreated) {
                    dbExists = true;
                    _Logger.success("Created database '" + databaseName + "'");
                } else {
                    _Logger.error("Could not create database ", databaseName);
                }
            }
        }
    } while (!dbExists);

    let isCollectionConfirmed = true;
    do {
        collectionName = await _confirmOrInput("Collection name", collectionName);
        isCollectionConfirmed = await _confirmAction("Confirm '" + collectionName + "'", true);
    } while (!isCollectionConfirmed);

    // const startTime = performance.now();
    const startTime = Date.now();
    sqlConfig.database = databaseName;

    // update sqlDOM with db name and collection name
    sqlRequestObject = {
        databaseName,
        tables: [{ name: collectionName, fields: [] }],
    };

    _Logger.success("Database : ", JSON.stringify(sqlRequestObject));

    _generateSQLObjectTablesFromCollection({
        collectionName,
        collectionData: jsonData,
    });

    _Logger.warning("Generating SQL Object Model File");
    fs.writeFileSync("sql_request_object.json", JSON.stringify(sqlRequestObject));
    // * Regenerate SQL
    // Write the SQL Object model in f ile
    fs.writeFileSync(
        "sql_request_object_tables.json",
        JSON.stringify(sqlRequestObject.tables.map((table) => ({ ...table, fields: table.fields.map((field) => ({ name: field.name, length: field.values.length })) })))
    );

    _Logger.warning("Generating SQL Script from SQL Object Model");
    let sqlScript = _generateSQLScript(sqlRequestObject);

    _Logger.warning("Executing SQL Script in ", sqlConfig.host, ":", sqlConfig.database);
    //  Drop table if exists
    _executeSQLScript(sqlScript);

    // ? Objects
    rl.close();
    _Logger.success("Converted JSON (" + fileName + ") to SQL in database '" + databaseName + "' in ", Date.now() - startTime, " ms");
}

main();

// class SQLRequestModel {
//     constructor(database, table = null){
//         this.database  = database;

//     }
// }
