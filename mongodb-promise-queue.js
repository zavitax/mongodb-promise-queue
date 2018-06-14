/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/

let crypto = require('crypto')


// ========================================================================================


// some helper functions
function id() {
    return crypto.randomBytes(16).toString('hex')
}

// ----------------------------------------------------------------------

function now() {
    return (new Date()).toISOString()
}

// ----------------------------------------------------------------------

function nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000)).toISOString()
}

// ----------------------------------------------------------------------

module.exports = function(mongoDbClient, name, opts) {
    return new Queue(mongoDbClient, name, opts)
}


// ========================================================================================


// the Queue object itself
function Queue(mongoDbClient, name, opts) {
    if ( !mongoDbClient ) {
        throw new Error("mongodb-queue: provide a mongodb.MongoClient")
    }
    if ( !name ) {
        throw new Error("mongodb-queue: provide a queue name")
    }
    opts = opts || {}

    this.name = name
    this.col = mongoDbClient.collection(name)
    this.visibility = opts.visibility || 30
    this.delay = opts.delay || 0

    if ( opts.deadQueue ) {
        this.deadQueue = opts.deadQueue
        this.maxRetries = opts.maxRetries || 5
    }
}

// ----------------------------------------------------------------------

// Queue.prototype.createIndexes = function(callback) {
Queue.prototype.createIndexes = function() {
    // let self = this

    // self.col.createIndex({ deleted : 1, visible : 1 }, function(err, indexname) {
    //     if (err) return callback(err)
    //     self.col.createIndex({ ack : 1 }, { unique : true, sparse : true }, function(err) {
    //         if (err) return callback(err)
    //         callback(null, indexname)
    //     })
    // })

    return this.col.createIndex({ deleted : 1, visible : 1 })
    .then(indexname => {
        return this.col.createIndex({ ack : 1 }, { unique : true, sparse : true })
        .then(() => indexname)
    })
}

// ----------------------------------------------------------------------

// Queue.prototype.add = function(payload, opts, callback) {
Queue.prototype.add = function(payload, opts) {
    // let self = this
    // if ( !callback ) {
    //     callback = opts
    //     opts = {}
    // }
    
    let delay = opts.delay || self.delay
    let visible = delay ? nowPlusSecs(delay) : now()

    let messages = []

    if (payload instanceof Array) {
        if (payload.length === 0) {
            let errMsg = 'Queue.add(): Array payload length must be greater than 0'
            // return callback(new Error(errMsg))
            throw new Error(errMsg)
        }

        payload.forEach(function(payload) {
            messages.push({
                visible  : visible,
                payload  : payload,
            })
        })
    } else {
        messages.push({
            visible  : visible,
            payload  : payload,
        })
    }

    // self.col.insertMany(messages, function(err, results) {
    //     if (err) return callback(err)
    //     if (payload instanceof Array) return callback(null, '' + results.insertedIds)
    //     callback(null, '' + results.ops[0]._id)
    // })

    return this.col.insertMany(messages)
    .then(results => {
        return payload instanceof Array ?
            results.insertedIds :
            results.ops[0]._id
    })
}

// ----------------------------------------------------------------------

// Queue.prototype.get = function(opts, callback) {
Queue.prototype.get = function(opts) {
    // let self = this
    // if ( !callback ) {
    //     callback = opts
    //     opts = {}
    // }

    let visibility = opts.visibility || self.visibility

    let query = {
        deleted : null,
        visible : { $lte : now() },
    }

    let sort = {
        _id : 1
    }
    
    let update = {
        $inc : { tries : 1 },
        $set : {
            ack     : id(),
            visible : nowPlusSecs(visibility),
        }
    }

    return this.col.findOneAndUpdate(query, update, { sort: sort, returnOriginal : false })
    .then((result) => {
        let msg = result.value

        if (!msg) return;

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id      : '' + msg._id,
            ack     : msg.ack,
            payload : msg.payload,
            tries   : msg.tries,
        }

        // if we have a deadQueue, then check the tries, else don't
        if ( this.deadQueue ) {
            // check the tries
            if ( msg.tries > self.maxRetries ) {
                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                return this.deadQueue.add(msg)
                .then(() => {
                    this.ack(msg.ack)
                })
                .then(() => {
                    this.get(opts)
                })
            }
        }

        return msg
    })

    // self.col.findOneAndUpdate(query, update, { sort: sort, returnOriginal : false }, function(err, result) {
    //     if (err) return callback(err)
    //     let msg = result.value
    //     if (!msg) return callback()

    //     // convert to an external representation
    //     msg = {
    //         // convert '_id' to an 'id' string
    //         id      : '' + msg._id,
    //         ack     : msg.ack,
    //         payload : msg.payload,
    //         tries   : msg.tries,
    //     }
    //     // if we have a deadQueue, then check the tries, else don't
    //     if ( self.deadQueue ) {
    //         // check the tries
    //         if ( msg.tries > self.maxRetries ) {
    //             // So:
    //             // 1) add this message to the deadQueue
    //             // 2) ack this message from the regular queue
    //             // 3) call ourself to return a new message (if exists)
    //             self.deadQueue.add(msg, function(err) {
    //                 if (err) return callback(err)
    //                 self.ack(msg.ack, function(err) {
    //                     if (err) return callback(err)
    //                     self.get(callback)
    //                 })
    //             })
    //             return
    //         }
    //     }

    //     callback(null, msg)
    // })
}

// ----------------------------------------------------------------------

// Queue.prototype.ping = function(ack, opts, callback) {
Queue.prototype.ping = function(ack, opts) {
    // let self = this
    // if ( !callback ) {
    //     callback = opts
    //     opts = {}
    // }

    let visibility = opts.visibility || self.visibility
    
    let query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }
    
    let update = {
        $set : {
            visible : nowPlusSecs(visibility)
        }
    }

    return this.col.findOneAndUpdate(query, update, { returnOriginal : false })
    .then(msg => {
        if ( !msg.value ) {
            throw new Error("Queue.ping(): Unidentified ack  : " + ack)
            // return callback(new Error("Queue.ping(): Unidentified ack  : " + ack))
        }

        return '' + msg.value._id
        // callback(null, '' + msg.value._id)
    })

    // self.col.findOneAndUpdate(query, update, { returnOriginal : false }, function(err, msg, blah) {
    //     if (err) return callback(err)
    //     if ( !msg.value ) {
    //         return callback(new Error("Queue.ping(): Unidentified ack  : " + ack))
    //     }
    //     callback(null, '' + msg.value._id)
    // })
}

// ----------------------------------------------------------------------

// Queue.prototype.ack = function(ack, callback) {
Queue.prototype.ack = function(ack) {
    // let self = this

    let query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }

    let update = {
        $set : {
            deleted : now(),
        }
    }

    return this.col.findOneAndUpdate(query, update, { returnOriginal : false })
    .then(msg => {
        if ( !msg.value ) {
            throw new Error("Queue.ack(): Unidentified ack : " + ack)
        }

        return '' + msg.value._id
    })

    // self.col.findOneAndUpdate(query, update, { returnOriginal : false }, function(err, msg, blah) {
    //     if (err) return callback(err)
    //     if ( !msg.value ) {
    //         return callback(new Error("Queue.ack(): Unidentified ack : " + ack))
    //     }
    //     callback(null, '' + msg.value._id)
    // })
}

// ----------------------------------------------------------------------

// Queue.prototype.clean = function(callback) {
Queue.prototype.clean = function() {
    // let self = this

    let query = {
        deleted : { $exists : true },
    }

    return this.col.deleteMany(query)
}

// ----------------------------------------------------------------------

// Queue.prototype.total = function(callback) {
Queue.prototype.total = function() {
    // let self = this

    return this.col.count()
}

// ----------------------------------------------------------------------

// Queue.prototype.size = function(callback) {
Queue.prototype.size = function() {
    // let self = this

    let query = {
        deleted : null,
        visible : { $lte : now() },
    }

    return this.col.count(query)
}

// ----------------------------------------------------------------------

// Queue.prototype.inFlight = function(callback) {
Queue.prototype.inFlight = function() {
    // let self = this

    let query = {
        ack     : { $exists : true },
        visible : { $gt : now() },
        deleted : null,
    }

    return this.col.count(query)
}

// ----------------------------------------------------------------------

// Queue.prototype.done = function(callback) {
Queue.prototype.done = function() {
    // let self = this

    let query = {
        deleted : { $exists : true },
    }

    return this.col.count(query)
}
