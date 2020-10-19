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
    const chunks = db.collection('chunks')
    const matches = db.collection('matches')
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

    app.get('/ner', (req,res)=> {
      threads.find(
        {Label: {$ne:0}}
      ).toArray().then(results => {
        res.render('ner.ejs', {NER: results})
      }).catch(error => {console.error(error)})
    })

    app.get('/ner/thread', (req,res) => {
      var threadMatches = matches.find(
        {id: {$eq: req.body}}
        ).toArray()
      var threadChunks = chunks.find(
        {id: {$eq: req.body}}
        ).toArray()
      var thread = threads.find(
        {_id: {$eq: req.body}}
        ).toArray()
      res.render('thread.ejs', {
        thread: thread,
        threadMatches: threadMatches,
        threadChunks: threadChunks
      })
    })

    /*db.collection('threads').aggregate([
        {$match: {Label: {$ne: 0}}},
        {$lookup: {
          from: "chunks",
          localField: "_id",
          foreignField: "id",
          as: "chunks"
          }
        },{
          $lookup: {
            from: "matches",
            localField: "_id",
            foreignField: "id",
            as: "matches"
          }
        }
      ])*/

    app.put('/names', (req,res)=>{
      threads.findOneAndUpdate(
        {_id: postid},
        {$set: {
          Label: req.body.button
        }}
      ).then(
        result => {res.redirect('/labelling')}
      ).catch(
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