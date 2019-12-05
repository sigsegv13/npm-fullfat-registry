var follow = require('follow')
var fs = require('fs')
var EE = require('events').EventEmitter
var util = require('util')
var url = require('url')
var path = require('path')
var tmp = path.resolve(__dirname, 'tmp')
var mkdirp = require('mkdirp')
var rimraf = require('rimraf')
var assert = require('assert')
var stream = require('stream')
var util = require('util')
var crypto = require('crypto')
var once = require('once')
var parse = require('parse-json-response')
var hh = require('http-https')
var crypto = require('crypto')
var pino = require('pino')
var debug = require('debug')

var version = require('./package.json').version
var ua = 'npm FullFat/' + version + ' node/' + process.version
var readmeTrim = require('npm-registry-readme-trim')

function formatArgs() {
  var args = arguments
  var useColors = this.useColors
  var name = this.namespace

  args[0] = new Date().toUTCString() + ' +' + debug.humanize(this.diff) + ' ' + name + ' ' + args[0]

  return args
}

debug.formatArgs = formatArgs

function randomValueHex (len) {
  // convert to hexadecimal format and
  // return required number of characters
  return crypto.randomBytes(Math.ceil(len/2)).toString('hex').slice(0,len)
}

function escapeSlashes (str) {
  return str.replace('/', '%2F')
}

var slice = [].slice

var getLogger = function(name) {
  return debug('fullfat:'+name)
}

// var logger = pino({ prettyPrint: true, level: process.env.LOG_LEVEL || 'info' })
var logger = getLogger('general')

util.inherits(FullFat, EE)

module.exports = FullFat

dbg = undefined

function FullFat(conf) {
  logger('Entered FullFat')
  if (!conf.skim || !conf.fat) {
    throw new Error('skim and fat database urls required')
  }

  this.skim = url.parse(conf.skim).href
  this.skim = this.skim.replace(/\/+$/, '')
  logger('FullFat: skim -', this.skim)

  var f = url.parse(conf.fat)
  this.fat = f.href
  this.fat = this.fat.replace(/\/+$/, '')
  logger('FullFat: fat -', this.fat)
  delete f.auth
  this.publicFat = url.format(f)
  this.publicFat = this.publicFat.replace(/\/+$/, '')
  logger('FullFat: publicFat -', this.publicFat)

  this.registry = null
  if (conf.registry) {
    this.registry = url.parse(conf.registry).href
    this.registry = this.registry.replace(/\/+$/, '')
    logger('FullFat: registry -', this.registry)
  }

  this.ua = conf.ua || ua
  this.inactivity_ms = conf.inactivity_ms || 1000 * 60 * 60
  this.seqFile = conf.seq_file
  this.writingSeq = false
  this.error = false
  this.since = 0
  this.follow = null

  // set to true to log missing attachments only.
  // otherwise, emits an error.
  this.missingLog = conf.missing_log || false

  this.whitelist = conf.whitelist || [ /.*/ ]

  this.tmp = conf.tmp
  if (!this.tmp) {
    var rand = crypto.randomBytes(6).toString('hex')
    this.tmp = path.resolve('npm-fullfat-tmp-' + process.pid + '-' + rand)
  }

  this.boundary = 'npmFullFat-' + crypto.randomBytes(6).toString('base64')

  this.readSeq(this.seqFile)
  logger('Leaving FullFat')
}

FullFat.prototype.readSeq = function(file) {
  logger('Entered readSeq')
  if (!this.seqFile) {
    logger('readSeq: no seq file config, calling start')
    process.nextTick(this.start.bind(this))
  } else {
    logger('readSeq: calling gotSeq')
    fs.readFile(file, 'ascii', this.gotSeq.bind(this))
  }
  logger('Leaving readSeq')
}

FullFat.prototype.gotSeq = function(er, data) {
  logger('Entered gotSeq')
  if (er && er.code === 'ENOENT') {
    logger('gotSeq: no seq file')
    data = '0'
  } else if (er) {
    logger('gotSeq: emit error')
    return this.emit('error', er)
  }

  data = +data || 0
  logger('gotSeq: data -', data)
  this.since = data
  logger('gotSeq: since -', this.since)
  logger('gotSeq: calling start')
  this.start()
  logger('Leaving gotSeq')
}

FullFat.prototype.start = function() {
  logger('Entered start')
  if (this.follow)
    return this.emit('error', new Error('already started'))

  this.emit('start')
  this.follow = follow({
    db: this.skim,
    since: this.since,
    inactivity_ms: this.inactivity_ms
  }, this.onchange.bind(this))
  this.follow.on('error', this.emit.bind(this, 'error'))
  logger('Leaving start')
}

FullFat.prototype._emit = function(ev, arg) {
  logger('Entered _emit')
  // Don't emit errors while writing seq
  if (ev === 'error' && this.writingSeq) {
    this.error = arg
  } else {
    EventEmitter.prototype.emit.apply(this, arguments)
  }
  logger('Leaving _emit')
}

FullFat.prototype.writeSeq = function() {
  logger('Entered writeSeq')
  var seq = +this.since
  if (this.seqFile && !this.writingSeq && seq > 0) {
    var data = seq + '\n'
    var file = this.seqFile + '.' + seq
    this.writingSeq = true
    fs.writeFile(file, data, 'ascii', function(writeEr) {
      var er = this.error
      if (er)
        this.emit('error', er)
      else if (!writeEr) {
        fs.rename(file, this.seqFile, function(mvEr) {
          this.writingSeq = false
          var er = this.error
          if (er)
            this.emit('error', er)
          else if (!mvEr)
            this.emit('sequence', seq)
        }.bind(this))
      }
    }.bind(this))
  }
  logger('Leaving writeSeq')
}

FullFat.prototype.onchange = function(er, change) {
  logger('Entered onchange')
  if (er) {
    logger('onchange: error')
    return this.emit('error', er)
  }

  if (!change.id) {
    logger('onchange: no change id for seq', change.seq)
    logger('Returning from onchange')
    return
  }

  this.pause()
  this.since = change.seq

  change.log = getLogger('Printing with unescaped slashes: ' + change.id)

  this.emit('change', change)

  if (change.deleted) {
    logger('onchange: calling delete')
    this.delete(change)
  } else {
    logger('onchange: calling getDoc')
    this.getDoc(change)
  }
  logger('Leaving onchange')
}

FullFat.prototype.retryReq = function retryReq(req, fallback) {
  var self = this

  req.setTimeout(this.inactivity_ms, function() {
    self.emit('retry', req.path)
    req.abort()
    fallback()
  })
}

FullFat.prototype.retryReqIn = function retryReqIn(req, fallback, timeout) {
  logger('registering timeout function for putDoc')
  var self = this

  req.setTimeout(timeout, function() {
    logger('hangling timeout!')
    self.emit('retry', req.path)
    req.abort()
    fallback()
  })
  logger('registered timeout function for putDoc')
}

FullFat.prototype.getDoc = function(change) {
  change.log('Entered getDoc')
  var q = '?revs=true&att_encoding_info=true'
  // if we have design document, then slash escaping is not situable.
  if (change.id.slice(0,7) == '_design'){
    var opt = url.parse(this.skim + '/' + change.id + q)
  }
  else {
    var opt = url.parse(this.skim + '/' + escapeSlashes(change.id) + q)
  }
  //var opt = url.parse(this.skim + '/' + encodeURIComponent(change.id) + q)
  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }

  change.log('getDoc: executing skim request for doc', change.id)
  var req = hh.get(opt)
  change.log('getDoc: setting skim request error event')
  req.on('error', this.emit.bind(this, 'error'))
  change.log('getDoc: setting skim request response event to call ongetdoc')
  req.on('response', parse(this.ongetdoc.bind(this, change)))
  change.log('Leaving getDoc')
}

FullFat.prototype.ongetdoc = function(change, er, data, res) {
  change.log('Entered ongetdoc')
  if (er) {
    change.log('ongetdoc: error')
    this.emit('error', er)
  } else {
    change.doc = data
    if (change.id.match(/^_design\//)) {
      change.log('ongetdoc: calling putDesign')
      this.putDesign(change)
    } else if (data.time && data.time.unpublished) {
      change.log('ongetdoc: calling unpublish')
      this.unpublish(change)
    } else {
      change.log('ongetdoc: calling putDoc')
      this.putDoc(change)
    }
  }
  change.log('Leaving ongetdoc')
}

FullFat.prototype.onFetchRev = function onFetchRev(change, er, f, res) {
  change.log('Entered', arguments.callee.name)
  change.fat = change.doc
  if (f._rev == undefined){
    change.fat._rev = '1-' + randomValueHex(32)
  } else{
    change.fat._rev = f._rev
  }

  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.unpublish = function unpublish(change) {
  change.log('Entered', arguments.callee.name)

  var opt = url.parse(this.fat + '/' + escapeSlashes(change.id))

  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    agent: false
  }
  var req = hh.get(opt)
  req.on('error', this.emit.bind(this, 'error'))
  req.on('response', parse(this.onFetchRev.bind(this, change)))

  this.retryReq(req, this.unpublish.bind(this, change))

  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.putDoc = function putDoc(change) {
  change.log('Entered', arguments.callee.name)
  var q = '?revs=true&att_encoding_info=true'
  var opt = url.parse(this.fat + '/' + escapeSlashes(change.id) + q)

  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  change.log(arguments.callee.name, ': executing fat request for doc', change.id)
  var req = hh.get(opt)
  change.log(arguments.callee.name, ': setting fat request error event')
  req.on('error', this.emit.bind(this, 'error'))
  change.log(arguments.callee.name, ': setting fat request response event to call onfatget')
  req.on('response', parse(this.onfatget.bind(this, change)))
  req.on('socket', function(){
    logger('socket is assigned to this request');
  })
  this.retryReqIn(req, this.putDoc.bind(this, change), 5000)
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.putDesign = function putDesign(change) {
  change.log('Entered', arguments.callee.name)
  var doc = change.doc
  this.pause()
  var opt = url.parse(this.fat + '/' + change.id + '?new_edits=false')
  var b = new Buffer(JSON.stringify(doc), 'utf8')
  opt.method = 'PUT'
  opt.headers = {
    'user-agent': this.ua,
    'content-type': 'application/json',
    'content-length': b.length,
    'connection': 'close'
  }

  var req = hh.request(opt)
  req.on('response', parse(this.onputdesign.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
  req.end(b)
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.onputdesign = function onputdesign(change, er, data, res) {
  change.log('Entered', arguments.callee.name)
  if (er)
    return this.emit('error', er)
  this.emit('putDesign', change, data)
  this.resume()
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.delete = function delete_(change) {
  change.log('Entered', arguments.callee.name)
  var name = escapeSlashes(change.id)

  var opt = url.parse(this.fat + '/' + name)
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  opt.method = 'HEAD'

  var req = hh.request(opt)
  req.on('response', this.ondeletehead.bind(this, change))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.ondeletehead = function ondeletehead(change, res) {
  change.log('Entered', arguments.callee.name)
  // already gone?  totally fine.  move on, nothing to delete here.
  if (res.statusCode === 404)
    return this.afterDelete(change)

  var rev = res.headers.etag.replace(/^"|"$/g, '')
  opt = url.parse(this.fat + '/' + escapeSlashes(change.id) + '?rev=' + rev)
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  opt.method = 'DELETE'
  var req = hh.request(opt)
  req.on('response', parse(this.ondelete.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.ondelete = function ondelete(change, er, data, res) {
  change.log('Entered', arguments.callee.name)
  if (er && er.statusCode === 404)
    this.afterDelete(change)
  else if (er)
    this.emit('error', er)
  else
    // scorch the earth! remove fully! repeat until 404!
    this.delete(change)
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.afterDelete = function afterDelete(change) {
  change.log('Entered', arguments.callee.name)
  this.emit('delete', change)
  this.resume()
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.onfatget = function onfatget(change, er, f, res) {
  change.log('Entered', arguments.callee.name)
  if (er && er.statusCode !== 404) {
    change.log(arguments.callee.name, ': Returning from onfatget on 404 error')
    return this.emit('error', er)
  }

  if (er) {
    f = JSON.parse(JSON.stringify(change.doc))
    f._attachments = {}
  }

  //f._attachments = f._attachments || {}
  f._attachments = {}
  change.fat = f
  change.log(arguments.callee.name, ': Calling merge')
  this.merge(change)
  change.log('Leaving', arguments.callee.name)
}


FullFat.prototype.merge = function merge(change) {
  change.log('Entered', arguments.callee.name)

  var s = change.doc
  var f = change.fat
  var fat_revision = f._rev

  dbg && change.log(arguments.callee.name, 'source', s)
  dbg && change.log(arguments.callee.name, 'fat', f)

  // if no versions in the skim record, then nothing to fetch
  if (!s.versions) {
    change.log(arguments.callee.name, ': no versions')
    change.log('Returning from merge with call to resume follow')
    return this.resume()
  }

  // Only fetch attachments if it's on the list.
  var pass = true
  if (this.whitelist.length) {
    change.log(arguments.callee.name, ': processing whitelist, length', this.whitelist.length)
    pass = false
    for (var i = 0; !pass && i < this.whitelist.length; i++) {
      change.log(arguments.callee.name, ': whitelist loop, index', i, ', element', this.whitelist[i])
      var w = this.whitelist[i]
      if (typeof w === 'string') {
        change.log(arguments.callee.name, ': whitelist loop, whitelist element type is "string"')
        pass = w === escapeSlashes(change.id)
      } else {
        change.log(arguments.callee.name, ': whitelist loop, whitelist element type is', typeof w)
        pass = w.exec(escapeSlashes(change.id))
      }
    }
    if (!pass) {
      change.log(arguments.callee.name, ': pass, no attachments')
      f._attachments = {}
      change.log('Returning from merge with call to fetchAll')
      return this.fetchAll(change, [], [])
    }
  }

  var need = []
  var changed = false
  for (var v in s.versions) {
    change.log(arguments.callee.name, ': skim version', v)
    var tgz = s.versions[v].dist.tarball
    change.log(arguments.callee.name, ': skim tgz', tgz)
    var att = path.basename(url.parse(tgz).pathname)
    change.log(arguments.callee.name, ': skim att', att)
    var ver = s.versions[v]
    f.versions = f.versions || {}

    if (!f.versions[v] || f.versions[v].dist.shasum !== ver.dist.shasum) {
      change.log(arguments.callee.name, ': fat missing skim version or shasum mismatch')
      f.versions[v] = s.versions[v]
      need.push(v)
      changed = true
    } else if (!f._attachments[att]) {
      change.log(arguments.callee.name, ': fat missing attachment for att')
      need.push(v)
      changed = true
    }
  }

  if (changed) {
    change.log(arguments.callee.name, 'CHANGED TRUE')
  }

  change.log(arguments.callee.name, ': need', need)

  // remove any versions that s removes, or which lack attachments
  for (var v in f.versions) {
    change.log(arguments.callee.name, ': processing fat versions that skim removes')
    if (!s.versions[v]) {
      change.log(arguments.callee.name, ': deleting fat version', v)
      delete f.versions[v]
    }
  }


  for (var a in f._attachments) {
    change.log(arguments.callee.name, ': fat attachment', a)
    var found = false
    for (var v in f.versions) {
      change.log(arguments.callee.name, ': attachment: fat version', v)
      var tgz = f.versions[v].dist.tarball
      change.log(arguments.callee.name, ': attachment: fat tgz', tgz)
      var b = path.basename(url.parse(tgz).pathname)
      change.log(arguments.callee.name, ': attachment: fat b', b)
      if (b === a) {
        change.log(arguments.callee.name, ': attachment: found existing attachment', b)
        found = true
        break
      }
    }
    if (!found) {
      change.log(arguments.callee.name, ': attachment: version not found, deleting fat attachment', a)
      delete f._attachments[a]
      changed = true
    }
  }

  change.log(arguments.callee.name, ': assigning to s._rev:', s._rev, 'value of f._rev:', f._rev)

  // s._rev = f._rev
  s._rev = fat_revision
  

  for (var k in s) {
    if (k !== '_attachments' && k !== 'versions') {
      if (changed)
        f[k] = s[k]
      else if (JSON.stringify(f[k]) !== JSON.stringify(s[k])) {
        f[k] = s[k]
        changed = true
      }
    }
  }

  changed = readmeTrim(f) || changed

  change.log('changes detected:', changed)

  if (!changed) {
    change.log(arguments.callee.name, ': nothing changed, calling resume')
    this.resume()
  } else {
    change.log(arguments.callee.name, ': changed, calling fetchAll')
    this.fetchAll(change, need, [])
  }

  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.put = function put(change, did) {
  change.log('Entered', arguments.callee.name)
  var f = change.fat
  change.did = did
  // at this point, all the attachments have been fetched into
  // {this.tmp}/{change.id}-{change.seq}/{attachment basename}
  // make a multipart PUT with all of the missing ones set to
  // follows:true
  var boundaries = []
  var boundary = this.boundary
  var bSize = 0

  var attSize = 0
  var atts = f._attachments = f._attachments || {}

  var new_atts = {}

  // It's important that we do everything in enumeration order,
  // because couchdb is a jerk, and ignores disposition headers.
  // Still include the filenames, though, so at least we dtrt.
  // did.forEach(function(att) {
  //   logger(arguments.callee.name, ': did begin: att name', att.name)
  //   atts[att.name] = {
  //     length: att.length,
  //     follows: true
  //   }
  //
  //   if (att.type)
  //     atts[att.name].type = att.type
  //
  //   logger(arguments.callee.name, ': did end: att name', att.name)
  // })

  did.forEach(function(att) {
    logger('put: did begin: att name', att.name)
    new_atts[att.name] = {
      length: att.length,
      follows: true
    }

    if (att.type)
      new_atts[att.name].type = att.type

    logger('put: did end: att name', att.name)
  })

  var send = []

  // Object.keys(atts).forEach(function (name) {
  Object.keys(new_atts).forEach(function (name) {
    logger('put: object keys begin: atts name', JSON.stringify(name))
    var att = new_atts[name]

    if (att.follows !== true) {
      logger('put: atts: does not follow')
      logger('Returning from put')
      return
    }

    send.push([name, att])
    // attSize += att.length
    //
    // var b = '\r\n--' + boundary + '\r\n' +
    //         'content-length: ' + att.length + '\r\n' +
    //         'content-disposition: attachment; filename=' +
    //         JSON.stringify(name) + '\r\n'
    //
    // if (att.type)
    //   b += 'content-type: ' + att.type + '\r\n'
    //
    // b += '\r\n'
    //
    // boundaries.push(b)
    // bSize += b.length
    logger('put: object keys end: atts name', JSON.stringify(name))
  })

  // // one last boundary at the end
  // var b = '\r\n--' + boundary + '--'
  // bSize += b.length
  // boundaries.push(b)

  change.log(arguments.callee.name, ': writing doc with rev:', f._rev)

  // put with new_edits=false to retain the same rev
  // this assumes that NOTHING else is writing to this database!
  // var p = url.parse(this.fat + '/' + encodeURIComponent(f.name) + '?new_edits=false')
  var p = url.parse(this.fat + '/' + escapeSlashes(f.name) + '?new_edits=false') // '?rev='+f._rev)
  delete f._revisions

  change.log(arguments.callee.name, ': url', p.pathname)

  p.method = 'PUT'

  // p.headers = {
  //   'user-agent': this.ua,
  //   'content-type': 'multipart/related;boundary="' + boundary + '"',
  //   'connection': 'close'
  // }

  p.headers = {
    'user-agent': this.ua,
    'content-type': 'application/json',
    agent: false
  }

  var doc = new Buffer(JSON.stringify(f), 'utf8')
  // var len = 0
  var len = doc.length

  p.headers['content-length'] = len

  // // now, for the document
  // var b = '--' + boundary + '\r\n' +
  //         'content-type: application/json\r\n' +
  //         'content-length: ' + doc.length + '\r\n\r\n'
  // bSize += b.length
  // 
  // p.headers['content-length'] = attSize + bSize + doc.length

  change.log(arguments.callee.name, ': executing put request')
  var req = hh.request(p)
  change.log(arguments.callee.name, ': setting request error event')
  req.on('error', this.emit.bind(this, 'error'))

  // change.log(arguments.callee.name, ': writing boundary')
  // req.write(b, 'ascii')
  // change.log(arguments.callee.name, ': writing doc:', JSON.stringigy(JSON.parse(doc), null, 2))
  // req.write(doc)
  // change.log(arguments.callee.name, ': calling putAttachments')
  // this.putAttachments(req, change, boundaries, send)
  // change.log(arguments.callee.name, ': setting request response event with onputres')
  // req.on('response', parse(this.onputres.bind(this, change)))

  dbg && change.log(arguments.callee.name, ': writing doc:', JSON.stringify(JSON.parse(doc), null, 2))

  req.on('response', parse(_onPutDoc.bind(this, change)))
  req.end(doc)

  function _onPutDoc(change, er, data, res) {
    change.log('put: entered', arguments.callee.name)

    dbg && change.log('put:', arguments.callee.name, ': data:', data )

    if (!change.id)
      throw new Error('wtf')

    // In some oddball cases, it looks like CouchDB will report stubs that
    // it doesn't in fact have.  It's possible that this is due to old bad
    // data in a past FullfatDB implementation, but whatever the case, we
    // ought to catch such errors and DTRT.  In this case, the "right thing"
    // is to re-try the PUT as if it had NO attachments, so that it no-ops
    // the attachments that ARE there, and fills in the blanks.
    // We do that by faking the onfatget callback with a 404 error.
    if (er && er.statusCode === 412 &&
        0 === er.message.indexOf('{"error":"missing_stub"')){

      change.log('put:', arguments.callee.name, ': 412 error missing stub')
      this.emit('error', er)

    } else if (er){
      this.emit('error', er)
    } else {

      _putAttachments.call(this, change, send, data.rev)

    }

    change.log('put: leaving', arguments.callee.name)
  }

  function _putAttachments(change, send, rev) {
    change.log('put: entered', arguments.callee.name)

    var done = []
    var idx = 0

    if (change.did.length !== 0) {
      change.log('put:', arguments.callee.name, ': calling _putOneAttachment')
      _putOneAttachment.call(this, 0, rev, _attachmentDone)
    } else {
      change.log('put:', arguments.callee.name, ': calling resume')
      this.resume()
    }

    function _putOneAttachment(idx, rev, doneCb) {
      change.log('put: _putAttachments: entered', arguments.callee.name)

      change.log(arguments.callee.name, 'idx:', idx, 'send:', send[idx])
      change.log(arguments.callee.name, 'rev:', rev)

      var f = change.fat

      var ns = send[idx]

      var name = ns[0]
      var att = ns[1]

      var p = url.parse(this.fat + '/' + escapeSlashes(f.name) + '/' + name + '?&rev='+rev)

      p.method = 'PUT'
      p.headers = {
        'user-agent': this.ua,
        'content-type': att.type|| 'application/octet-steam',
        agent: false
      }

      var file = path.join(this.tmp, escapeSlashes(change.id) + '-' + change.seq, name)
      //var data = fs.readFileSync(file)
      var rs = fs.createReadStream(file)

      var req = hh.request(p)
      req.on('error', this.emit.bind(this, 'error'))

      change.log('writing attachment', p.path)
      req.on('response', parse(doneCb.bind(this, name, att, idx)))

      rs.pipe(req)
      //req.end(data)

      // XXX req.setTimeout()
      change.log('put: _putAttachments: leaving', arguments.callee.name)
    }


    function _attachmentDone(name, att, idx, er, data, res) {
      change.log('put: _putAttachments: entered', arguments.callee.name)

      dbg && change.log('put: _putAttachments:', arguments.callee.name, 'name', name, 'att', att, 'idx', idx, 'er', er, 'data', data)

      if (er){
        change.log('put: _putAttachments:', arguments.callee.name, ': emit error')
        this.emit('error', er)
      } else {
        change.log('put: _putAttachments:', arguments.callee.name, ': emit putAttachment')
        this.emit('putAttachment', change, data, name, att)

        done.push(name)

        if(done.length === send.length){

          change.log('put: _putAttachments:', arguments.callee.name, ': attachments done:', done)

          rimraf(this.tmp + '/' + escapeSlashes(change.id) + '-' + change.seq, function(err){
            change.log('put: _putAttachments:', arguments.callee.name, ': rmrf done:', err)
          })

          change.log('put: _putAttachments:', arguments.callee.name, ': emit put')
          this.emit('put', change, done)

          change.log('put: _putAttachments:', arguments.callee.name, ': calling resume')
          this.resume()
        } else {
          change.log('put: _putAttachments:', arguments.callee.name, ': calling _putOneAttachment')
          _putOneAttachment.call(this, idx+1, data.rev, _attachmentDone)
        }
      }
      change.log('put: _putAttachments: leaving', arguments.callee.name)
    }

    change.log('put: leaving', arguments.callee.name)
  }

  change.log('Leaving put')
}

FullFat.prototype.putAttachments0 = function putAttachments0(req, change, boundaries, send) {
  change.log('Entered', arguments.callee.name)
  // send is the ordered list of [[name, attachment object],...]
  var b = boundaries.shift()
  var ns = send.shift()

  // last one!
  if (!ns) {
    change.log(arguments.callee.name, ': no ns, last, writing boundary')
    req.write(b, 'ascii')
    change.log('Returning from putAttachments with request end()')
    return req.end()
  }

  var name = ns[0]
  change.log(arguments.callee.name, ': name', name, 'start')
  req.write(b, 'ascii')
  var file = path.join(this.tmp, escapeSlashes(change.id) + '-' + change.seq, name)
  change.log(arguments.callee.name, ': file', file)
  change.log(arguments.callee.name, ': creating read stream for file', file)
  var data = fs.readFileSync(file)
  //var fstr = fs.createReadStream(file)

  change.log(arguments.callee.name, ': writing request data')
  // fstr.on('end', function() {
  req.write(data, function() {
    logger('putAttachments: upload for', name, 'done')
    this.emit('upload', {
      change: change,
      name: name
    })
    logger('putAttachments: calling putAttachments')
    this.putAttachments(req, change, boundaries, send)
  }.bind(this))

  // logger('putAttachments: setting file stream error event')
  // fstr.on('error', this.emit.bind(this, 'error'))
  // fstr.pipe(req, { end: false })
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.onputdoc = function onputdoc(change, er, data, res) {
}

FullFat.prototype.onputres = function onputres(change, er, data, res) {
  change.log('Entered', arguments.callee.name)

  if (!change.id)
    throw new Error('wtf?')

  // In some oddball cases, it looks like CouchDB will report stubs that
  // it doesn't in fact have.  It's possible that this is due to old bad
  // data in a past FullfatDB implementation, but whatever the case, we
  // ought to catch such errors and DTRT.  In this case, the "right thing"
  // is to re-try the PUT as if it had NO attachments, so that it no-ops
  // the attachments that ARE there, and fills in the blanks.
  // We do that by faking the onfatget callback with a 404 error.
  if (er && er.statusCode === 412 &&
      0 === er.message.indexOf('{"error":"missing_stub"') &&
      !change.didFake404){
    change.log(arguments.callee.name, ': missing stub error, calling onfatget with status code 404')
    change.didFake404 = true
    this.onfatget(change, { statusCode: 404 }, {}, {})
  } else if (er) {
    change.log(arguments.callee.name, ': emit error')
    this.emit('error', er)
  } else {
    change.log(arguments.callee.name, ': emit put')
    this.emit('put', change, data)
    // Just a best-effort cleanup.  No big deal, really.
    change.log(arguments.callee.name, ': calling rimraf for cleanup')
    rimraf(this.tmp + '/' + escapeSlashes(change.id) + '-' + change.seq, function() {})
    change.log(arguments.callee.name, ': calling resume')
    this.resume()
  }
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.fetchAll = function fetchAll(change, need, did) {
  change.log('Entered', arguments.callee.name)
  var f = change.fat
  change.log(arguments.callee.name, ': tmp component this.tmp:', this.tmp)
  change.log(arguments.callee.name, ': tmp component change.id:', change.id)
  change.log(arguments.callee.name, ': tmp component change.seq:', change.seq)
  var tmp = path.resolve(this.tmp, escapeSlashes(change.id) + '-' + change.seq)
  var len = need.length
  if (!len) {
    change.log(arguments.callee.name, ': Returning from fetchAll, nothing to fetch')
    return this.put(change, did)
  }

  var errState = null

  change.log(arguments.callee.name, ': creating tmp dir:', tmp)
  mkdirp(tmp, function(er) {
    if (er) {
      logger('Returning from fetchAll on error to create tmp dir')
      return this.emit('error', er)
    }
    logger('fechAll: Binding fetchOne for each item in need')
    need.forEach(this.fetchOne.bind(this, change, need, did))
  }.bind(this))
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.fetchOne = function fetchOne(change, need, did, v) {
  change.log('Entered', arguments.callee.name)
  var f = change.fat
  change.log(arguments.callee.name, ': version (v):', v)
  var r = url.parse(change.doc.versions[v].dist.tarball)
  change.log(arguments.callee.name, ': url based on skim doc (r):', r.pathname)
  if (this.registry) {
    change.log(arguments.callee.name, ': url based on registry')
    var p = '/' + escapeSlashes(change.id) + '/-/' + path.basename(r.pathname)
    change.log(arguments.callee.name, ': registry: path (p):', p)
    r = url.parse(this.registry + p)
    change.log(arguments.callee.name, ': url based on registry (r):', r)
  }

  r.method = 'GET'
  r.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }

  change.log(arguments.callee.name, ': executing request')
  var req = hh.request(r)
  change.log(arguments.callee.name, ': setting request error event')
  req.on('error', this.emit.bind(this, 'error'))
  change.log(arguments.callee.name, ': setting request response event to call onattres')
  req.on('response', this.onattres.bind(this, change, need, did, v, r))
  req.end()
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.onattres = function onattres(change, need, did, v, r, res) {
  change.log('Entered', arguments.callee.name)
  var f = change.fat
  var att = r.href
  change.log(arguments.callee.name, ': att:', att)
  var sum = f.versions[v].dist.shasum
  change.log(arguments.callee.name, ': fat shasum:', sum)
  var filename = escapeSlashes(f.name) + '-' + v + '.tgz'
  change.log(arguments.callee.name, ': fat filename:', filename)
  var file = path.join(this.tmp, escapeSlashes(change.id) + '-' + change.seq, filename)
  change.log(arguments.callee.name, ': fat file:', file)

  // TODO: If the file already exists, get its size.
  // If the size matches content-length, get the md5
  // If the md5 matches content-md5, then don't bother downloading!

  function skip() {
    logger('onattres: entered skip')
    rimraf(file, function() {})
    delete f.versions[v]
    if (f._attachments)
      delete f._attachments[file]
    need.splice(need.indexOf(v), 1)
    maybeDone(null)
    logger('onattres: leaving skip')
  }

  var maybeDone = function maybeDone(a) {
    logger('onattres: entered maybeDone')
    if (a)
      this.emit('download', a)
    if (need.length === did.length)
      this.put(change, did)
    logger('onattres: leaving maybeDone')
  }.bind(this)

  // if the attachment can't be found, then skip that version
  // it's uninstallable as of right now, and may or may not get
  // fixed in a future update
  if (res.statusCode !== 200) {
    change.log(arguments.callee.name, ': attachment not found, status code', res.statusCode)
    var er = new Error('Error fetching attachment: ' + att)
    er.statusCode = res.statusCode
    er.code = 'attachment-fetch-fail'
    if (this.missingLog) {
      change.log('Returning from onattres with an append to missing log')
      return fs.appendFile(this.missingLog, att + '\n', skip)
    } else {
      change.log('Returning from onattres with an error')
      return this.emit('error', er)
    }
  }

  var fstr = fs.createWriteStream(file)

  // check the shasum while we're at it
  var sha = crypto.createHash('sha1')
  var shaOk = false
  var errState = null

  sha.on('data', function(c) {
    logger('onattres: entered sha on data')
    c = c.toString('hex')
    if (c === sum)
      shaOk = true
    logger('onattres: leaving sha event')
  }.bind(this))

  if (!res.headers['content-length']) {
    var counter = new Counter()
    res.pipe(counter)
  }

  res.pipe(sha)
  res.pipe(fstr)

  fstr.on('error', function(er) {
    logger('onattres: entered fstr on error')
    er.change = change
    er.version = v
    er.path = file
    er.url = att
    this.emit('error', errState = errState || er)
    logger('onattres: leaving fstr event')
  }.bind(this))

  fstr.on('close', function() {
    logger('onattres: entered fstr on close')
    if (errState || !shaOk) {
      // something didn't work, but the error was squashed
      // take that as a signal to just delete this version
      logger('onattres: returning from fstr close event with skip()')
      return skip()
    }
    // it worked!  change the dist.tarball url to point to the
    // registry where this is being stored.  It'll be rewritten by
    // the _show/pkg function when going through the rewrites, anyway,
    // but this url will work if the couch itself is accessible.
    // var filename = path.basename(f.name) + '-' + v + '.tgz'
    var newatt = this.publicFat + '/' + escapeSlashes(change.id) + '/' + escapeSlashes(change.id) + '-' + v + '.tgz'
    logger('onattres: fstr close event: newatt', newatt)
    f.versions[v].dist.tarball = newatt

    if (res.headers['content-length'])
      var cl = +res.headers['content-length']
    else
      var cl = counter.count

    var a = {
      change: change,
      version: v,
      name: path.basename(file),
      length: cl,
      type: res.headers['content-type']
    }
    logger('onattres: fstr close event: calling did.push')
    did.push(a)
    logger('onattres: fstr close event: calling maybeDone')
    maybeDone(a)

    logger('onattres: leaving fstr close event')
  }.bind(this))
  change.log('Leaving', arguments.callee.name)
}

FullFat.prototype.destroy = function() {
  logger('Entered destroy')
  if (this.follow)
    this.follow.die()
  logger('Leaving destroy')
}

FullFat.prototype.pause = function() {
  logger('Entered pause')
  if (this.follow) {
    logger('pause: pausing follow')
    this.follow.pause()
  }
  logger('Leaving pause')
}

FullFat.prototype.resume = function() {
  logger('Entered resume')
  this.writeSeq()
  if (this.follow) {
    logger('resume: resume follow')
    this.follow.resume()
  }
  logger('Leaving resume')
}

util.inherits(Counter, stream.Writable)
function Counter(options) {
  logger('Entered Counter')
  stream.Writable.call(this, options)
  this.count = 0
  logger('Leaving Counter')
}
Counter.prototype._write = function(chunk, encoding, cb) {
  logger('Entered Counter _write')
  this.count += chunk.length
  cb()
  logger('Leaving Counter _write')
}
