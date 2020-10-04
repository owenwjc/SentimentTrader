const http = require('http');
const express = require('express');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient
const app = express();

MongoClient.connect('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false',{
  useUnifiedTopology: true}).then(client =>{
    console.log('In the mainframe')
    const db = client.db('db')
    const testCollection = db.collection('Test Collection')
    app.use(bodyParser.urlencoded({extended:true}))

    app.listen(3000,function(){
      console.log('listening on 3000')
    })

    app.get('/',function(req,res){
      res.sendFile(__dirname + '/express' + '/index.html')
    })

    app.post('/direction', (req,res) => {
      var obj = {button: req.body.button, name: req.body.name}
      testCollection.insertOne(obj).then(
        result => {
          res.redirect('/')
        }).catch(
          error => console.error(error)
        )
    })
  }
).catch(console.error)