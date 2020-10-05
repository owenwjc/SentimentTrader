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

    app.set('view engine', 'ejs')
    app.use(express.static('public'))
    app.use(bodyParser.urlencoded({extended:true}))
    app.use(bodyParser.json())

    app.listen(3000,function(){
      console.log('listening on 3000')
    })

    app.get('/', (req,res) => {
      testCollection.find().toArray().then(results => {
        res.render('index.ejs', {names: results})
      }).catch(error => {console.error(error)})
    })

    app.post('/direction', (req,res) => {
      var obj = {button: req.body.button, name: req.body.name}
      testCollection.insertOne(obj).then(
        result => {
          res.redirect('/')
        }).catch(
          error => {console.error(error)}
        )
    })

    app.put('/names', (req,res)=>{
      testCollection.findOneAndUpdate(
        {name: 'Owen'},
        {$set: {
          name: req.body.name,
          button: req.body.button
        }},
        {
          upsert: true
        }
      ).then(
        result => {console.log(result)}
      ).catch(
        error => {console.error(error)}
      )
    })

  }
).catch(console.error)