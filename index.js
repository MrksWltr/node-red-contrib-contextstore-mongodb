const { MongoClient } = require('mongodb')

class MongoDbContextStore {
    constructor(config) {

        this.config = config;

        this.uri = ''.concat(
            "mongodb://"
            , config.user
            , ":"
            , config.password
            , "@"
            , config.host
            , ":"
            , config.port
            , "/admin"
        )

        console.log(this.uri)

        this.client = new MongoClient(this.uri)

        console.log("mongodb context initialized")
    }

    async open() {
        //Open the storage ready for use

        return this.client.connect()
        .then(() => {
            console.log("mongodb context store connected")
        })
    }

    async close() {
        //Close the storage

        return this.client.close()
        .then(() => {
            console.log("mongodb context store closed")
        })
    }

    async get(scope, key, callback) {
        //Get values from the store

        //some checks
        //mongodb uses asynchronous javascript api, so the caller have to do the same
        if (callback === undefined || typeof callback !== "function") {
            throw new Error("context store don't support synchronous access")
        }

        const db = this.client.db(this.config.database)

        var collection = ""
        var query = { }

        if (String(scope).toUpperCase() === 'GLOBAL') {

            collection = "ctxglobal"

            if (Array.isArray(key)) {
                query = { "key": { "$in": key } }
            } else {
                query = { "key": key }
            }

        } else {

            if (Array.isArray(key)) {
                query = { "id": scope, "key": { "$in": key } }
            } else {
                query = { "id": scope, "key": key }
            }

            if (String(scope).includes(':')) {
                //context of node

                collection = "ctxnode"
            } else {
                //context of flow

                collection = "ctxflow"
            }
        }

        if (Array.isArray(key)) {
            throw new Error("Array mode is not implemented currently")
        } else {
            return db.collection(collection)
            .findOne(query)
            .then((doc) => {
                //was a value available?
                if (doc !== undefined) {
                    callback(undefined, doc.value)
                } else {
                    callback(undefined, undefined)
                }
            })
            .catch((err) => {
                callback(err, undefined)
            })
        }
    }

    async set(scope, key, value, callback) {
        //Set values in the store

        if (callback === undefined || typeof callback !== "function") {
            throw new Error("context store don't support synchronous access")
        }

        var collection = ""

        const db = this.client.db(this.config.database)

        if (String(scope).toUpperCase() === "GLOBAL") {
            collection = "ctxglobal"
        } else {

            if (String(scope).includes(":")) {
                collection = "ctxnode"
            } else {
                collection = "ctxflow"
            }
        }

        if (!Array.isArray(key)) {
            
            const filter = { 
                id: scope,
                key: key
            }
            const options = { upsert: true }
            const updateDoc = {
                $set: {
                    value: value
                }
            }
            
            return db.collection(collection).updateOne(filter, updateDoc, options)
            .then(function() {
                callback(undefined)
            })
            .catch(function(err) {
                callback(err, undefined)
            })

        } else {
            
        }
    }

    async keys (scope, callback) {
        //Get a list of all keys in the store

        //some checks
        //mongodb uses asynchronous javascript api, so the caller have to do the same
        if (callback === undefined || typeof callback !== "function") {
            throw new Error("context store don't support synchronous access")
        }

        var ret = []
        var collection = ""
        var query = undefined

        var db = this.client.db(this.config.database)

        if (String(scope).toUpperCase() === "GLOBAL") {
            collection = "ctxglobal"
            query = { }
        } else {
            query = { "id": scope }
            if (String(scope).includes(":")) {
                collection = "ctxnode"
            } else {
                collection = "ctxflow"
            }
        }

        var c = db.collection(collection).find(query).project({ _id: 0, value: 0 }).toArray()
        .then((list) => {
            for (const i of list) {
                if (i !== undefined) {
                    ret.push(i.key)
                }
            }

            callback(undefined, ret)
        })
        .catch((err) => {
            callback(err, undefined)
        })
    }

    async delete (scope) {
        //Delete all keys for a given scope

        var collection = ""
        var query = undefined

        var db = this.client.db(this.config.database)

        if (String(scope).toUpperCase() === "GLOBAL") {
            collection = "ctxglobal"
            query = { }
        } else {
            query = { "id": scope}

            if (String(scope).includes(":")) {
                collection = "ctxglobal"
            } else {
                collection = "ctxglobal"
            }
        }

        return db.collection(collection).deleteMany(query)
    }

    async clean  (activeNodes) {
        //Clean the context store
    }
}

module.exports = MongoDbContextStore