const http = require('http');
const express = require('express');
const bodyParser = require('body-parser');
const MongoClient = require('mongodb').MongoClient
const app = express();
const ObjectID = require('mongodb').ObjectID

MongoClient.connect('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false',{
  useUnifiedTopology: true}).then(client =>{
    console.log('In the mainframe')
    const db = client.db('db')
    const testCollection = db.collection('Test Collection')
    var postid = null

    app.set('view engine', 'ejs')
    app.use(express.static('public'))
    app.use(bodyParser.urlencoded({extended:true}))
    app.use(bodyParser.json())

    app.listen(3000,function(){
      console.log('listening on 3000')
    })

    app.get('/', (req,res) => {
      testCollection.findOne(
          {tag: 0}
      ).then(results => {
        if (results){
          postid = results._id
          console.log(postid)
          res.render('home.ejs', {post: results})
        }
        else{
          res.render('empty.ejs')
          postid = null
        }
      }).catch(error => {console.error(error)})
    })

    app.put('/names', (req,res)=>{
      testCollection.findOneAndUpdate(
        {_id: postid},
        {$set: {
          tag: req.body.button
        }}
      ).then(
        result => {console.log(result)}
      ).catch(
        error => {console.error(error)}
      )
    })

    app.post('/direction', (req,res) => {
        var obj = {button: req.body.button, name: req.body.name, tag: 0}
        testCollection.insertOne(obj).then(
          result => {
            res.redirect('/')
          }).catch(
            error => {console.error(error)}
          )
    })

    app.delete('/names', (req,res) => {
        testCollection.deleteOne({
          _id: ObjectID(req.body._id)}
      ).then(result => {
        res.json('Deleted entry')
      }).catch(error => {console.error(error)})
    })


}).catch(console.error)