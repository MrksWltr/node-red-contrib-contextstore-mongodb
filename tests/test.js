const MongoDbContextStore = require('../index.js');

const store = new MongoDbContextStore({
    host: "localhost",
    port: 27017,
    database: "jctds",
    user: "jctds",
    password: "jctds"
})

