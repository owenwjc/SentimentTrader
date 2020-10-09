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
    const threads = db.collection('threads')
    var postid = null

    app.set('view engine', 'ejs')
    app.use(express.static('public'))
    app.use(bodyParser.urlencoded({extended:true}))
    app.use(bodyParser.json())

    app.listen(3000,function(){
      console.log('listening on 3000')
    })

    app.get('/',(req,res) => {
      res.render('home.ejs')
    })

    app.get('/labelling', (req,res) => {
      threads.findOne(
          {Label: 0}
      ).then(results => {
        if (results){
          postid = results._id
          res.render('labelling.ejs', {post: results})
        }
        else{
          res.render('empty.ejs')
          postid = null
        }
      }).catch(error => {console.error(error)})
    })

    app.put('/names', (req,res)=>{
      threads.findOneAndUpdate(
        {_id: postid},
        {$set: {
          Label: req.body.button
        }}
      ).then(
        result => {console.log(result)}
      ).catch(
        error => {console.error(error)}
      )
    })

    app.post('/direction', (req,res) => {
        var obj = {button: req.body.button, name: req.body.name, tag: 0}
        threads.insertOne(obj).then(
          result => {
            res.redirect('/')
          }).catch(
            error => {console.error(error)}
          )
    })

    app.delete('/names', (req,res) => {
        threads.deleteOne({
          _id: ObjectID(postid)}
      ).then(result => {
        res.json('Deleted entry')
      }).catch(error => {console.error(error)})
    })


}).catch(console.error)