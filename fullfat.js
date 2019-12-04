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
var pino = require('pino')
var debug = require('debug')

var version = require('./package.json').version
var ua = 'npm FullFat/' + version + ' node/' + process.version
var readmeTrim = require('npm-registry-readme-trim')

var slice = [].slice

var logger = pino({ prettyPrint: true, level: process.env.LOG_LEVEL || 'info' })

var getLogger = function(name) {
  return debug('fullfat:'+name)
}

util.inherits(FullFat, EE)

module.exports = FullFat

function FullFat(conf) {
  logger.debug('Entered FullFat')
  if (!conf.skim || !conf.fat) {
    throw new Error('skim and fat database urls required')
  }

  this.skim = url.parse(conf.skim).href
  this.skim = this.skim.replace(/\/+$/, '')
  logger.debug('FullFat: skim - "%s"', this.skim)

  var f = url.parse(conf.fat)
  this.fat = f.href
  this.fat = this.fat.replace(/\/+$/, '')
  logger.debug('FullFat: fat - "%s"', this.fat)
  delete f.auth
  this.publicFat = url.format(f)
  this.publicFat = this.publicFat.replace(/\/+$/, '')
  logger.debug('FullFat: publicFat - "%s"', this.publicFat)

  this.registry = null
  if (conf.registry) {
    this.registry = url.parse(conf.registry).href
    this.registry = this.registry.replace(/\/+$/, '')
    logger.debug('FullFat: registry - "%s"', this.registry)
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
  logger.debug('Leaving FullFat')
}

FullFat.prototype.readSeq = function(file) {
  logger.debug('Entered readSeq')
  if (!this.seqFile) {
    logger.debug('readSeq: no seq file config, calling start')
    process.nextTick(this.start.bind(this))
  } else {
    logger.debug('readSeq: calling gotSeq')
    fs.readFile(file, 'ascii', this.gotSeq.bind(this))
  }
  logger.debug('Leaving readSeq')
}

FullFat.prototype.gotSeq = function(er, data) {
  logger.debug('Entered gotSeq')
  if (er && er.code === 'ENOENT') {
    logger.debug('gotSeq: no seq file')
    data = '0'
  } else if (er) {
    logger.debug('gotSeq: emit error')
    return this.emit('error', er)
  }

  data = +data || 0
  logger.debug('gotSeq: data - "%d"', data)
  this.since = data
  logger.debug('gotSeq: since - "%d"', this.since)
  logger.debug('gotSeq: calling start')
  this.start()
  logger.debug('Leaving gotSeq')
}

FullFat.prototype.start = function() {
  logger.debug('Entered start')
  if (this.follow)
    return this.emit('error', new Error('already started'))

  this.emit('start')
  this.follow = follow({
    db: this.skim,
    since: this.since,
    inactivity_ms: this.inactivity_ms
  }, this.onchange.bind(this))
  this.follow.on('error', this.emit.bind(this, 'error'))
  logger.debug('Leaving start')
}

FullFat.prototype._emit = function(ev, arg) {
  logger.debug('Entered _emit')
  // Don't emit errors while writing seq
  if (ev === 'error' && this.writingSeq) {
    this.error = arg
  } else {
    EventEmitter.prototype.emit.apply(this, arguments)
  }
  logger.debug('Leaving _emit')
}

FullFat.prototype.writeSeq = function() {
  logger.debug('Entered writeSeq')
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
  logger.debug('Leaving writeSeq')
}

FullFat.prototype.onchange = function(er, change) {
  logger.debug('Entered onchange')
  if (er) {
    logger.debug('onchange: error')
    return this.emit('error', er)
  }

  if (!change.id) {
    logger.debug('onchange: no change id for seq "%d"', change.seq)
    logger.debug('Returning from onchange')
    return
  }

  this.pause()
  this.since = change.seq

  change.log = getLogger(change.id)

  this.emit('change', change)

  if (change.deleted) {
    logger.debug('onchange: calling delete')
    this.delete(change)
  } else {
    logger.debug('onchange: calling getDoc')
    this.getDoc(change)
  }
  logger.debug('Leaving onchange')
}

FullFat.prototype.getDoc = function(change) {
  logger.debug('Entered getDoc')
  change.log('getDoc')
  var q = '?revs=true&att_encoding_info=true'
  var opt = url.parse(this.skim + '/' + encodeURIComponent(change.id) + q)
  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }

  logger.debug('getDoc: executing skim request for doc "%s"', change.id)
  var req = hh.get(opt)
  logger.debug('getDoc: setting skim request error event')
  req.on('error', this.emit.bind(this, 'error'))
  logger.debug('getDoc: setting skim request response event to call ongetdoc')
  req.on('response', parse(this.ongetdoc.bind(this, change)))
  logger.debug('Leaving getDoc')
}

FullFat.prototype.ongetdoc = function(change, er, data, res) {
  logger.debug('Entered ongetdoc')
  change.log('ongetdoc')
  if (er) {
    logger.debug('ongetdoc: error')
    this.emit('error', er)
  } else {
    change.doc = data
    if (change.id.match(/^_design\//)) {
      logger.debug('ongetdoc: calling putDesign')
      this.putDesign(change)
    } else if (data.time && data.time.unpublished) {
      logger.debug('ongetdoc: calling unpublish')
      this.unpublish(change)
    } else {
      logger.debug('ongetdoc: calling putDoc')
      this.putDoc(change)
    }
  }
  logger.debug('Leaving ongetdoc')
}

FullFat.prototype.unpublish = function unpublish(change) {
  logger.debug('Entered unpublish')
  change.log(arguments.callee.name)
  change.fat = change.doc
  this.put(change, [])
  logger.debug('Leaving unpublish')
}

FullFat.prototype.putDoc = function putDoc(change) {
  logger.debug('Entered putDoc')
  change.log(arguments.callee.name)
  var q = '?revs=true&att_encoding_info=true'
  var opt = url.parse(this.fat + '/' + encodeURIComponent(change.id) + q)

  opt.method = 'GET'
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  logger.debug('putDoc: executing fat request for doc "%s"', change.id)
  var req = hh.get(opt)
  logger.debug('putDoc: setting fat request error event')
  req.on('error', this.emit.bind(this, 'error'))
  logger.debug('putDoc: setting fat request response event to call onfatget')
  req.on('response', parse(this.onfatget.bind(this, change)))
  logger.debug('Leaving putDoc')
}

FullFat.prototype.putDesign = function putDesign(change) {
  logger.debug('Entered putDesign')
  change.log(arguments.callee.name)
  var doc = change.doc
  this.pause()
  var opt = url.parse(this.fat + '/' + encodeURIComponent(change.id) + '?new_edits=false')
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
  logger.debug('Entered putDesign')
}

FullFat.prototype.onputdesign = function onputdesign(change, er, data, res) {
  logger.debug('Entered onputdesign')
  change.log(arguments.callee.name)
  if (er)
    return this.emit('error', er)
  this.emit('putDesign', change, data)
  this.resume()
  logger.debug('Leaving onputdesign')
}

FullFat.prototype.delete = function delete_(change) {
  logger.debug('Entered delete')
  change.log(arguments.callee.name)
  var name = change.id

  var opt = url.parse(this.fat + '/' + encodeURIComponent(name))
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  opt.method = 'HEAD'

  var req = hh.request(opt)
  req.on('response', this.ondeletehead.bind(this, change))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()
  logger.debug('Leaving delete')
}

FullFat.prototype.ondeletehead = function ondeletehead(change, res) {
  logger.debug('Entered ondeletehead')
  change.log(arguments.callee.name)
  // already gone?  totally fine.  move on, nothing to delete here.
  if (res.statusCode === 404)
    return this.afterDelete(change)

  var rev = res.headers.etag.replace(/^"|"$/g, '')
  opt = url.parse(this.fat + '/' + encodeURIComponent(change.id) + '?rev=' + rev)
  opt.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }
  opt.method = 'DELETE'
  var req = hh.request(opt)
  req.on('response', parse(this.ondelete.bind(this, change)))
  req.on('error', this.emit.bind(this, 'error'))
  req.end()
  logger.debug('Leaving ondeletehead')
}

FullFat.prototype.ondelete = function ondelete(change, er, data, res) {
  logger.debug('Entered ondelete')
  change.log(arguments.callee.name)
  if (er && er.statusCode === 404)
    this.afterDelete(change)
  else if (er)
    this.emit('error', er)
  else
    // scorch the earth! remove fully! repeat until 404!
    this.delete(change)
  logger.debug('Leaving ondelete')
}

FullFat.prototype.afterDelete = function afterDelete(change) {
  logger.debug('Entered afterdelete')
  change.log(arguments.callee.name)
  this.emit('delete', change)
  this.resume()
  logger.debug('Leaving afterdelete')
}

FullFat.prototype.onfatget = function onfatget(change, er, f, res) {
  logger.debug('Entered onfatget')
  change.log(arguments.callee.name)
  if (er && er.statusCode !== 404) {
    logger.debug('onfatget: Returning from onfatget on 404 error')
    return this.emit('error', er)
  }

  if (er)
    f = JSON.parse(JSON.stringify(change.doc))

  f._attachments = f._attachments || {}
  change.fat = f
  logger.debug('onfatget: Calling merge')
  this.merge(change)
  logger.debug('Leaving onfatget')
}


FullFat.prototype.merge = function merge(change) {
  logger.debug('Entered merge')
  change.log(arguments.callee.name)
  var s = change.doc
  var f = change.fat

  // if no versions in the skim record, then nothing to fetch
  if (!s.versions) {
    logger.debug('merge: no versions')
    logger.debug('Returning from merge with call to resume follow')
    change.log('no versions')
    return this.resume()
  }

  // Only fetch attachments if it's on the list.
  var pass = true
  if (this.whitelist.length) {
    logger.debug('merge: whitelist length "%d"', this.whitelist.length)
    change.log('processing whitelist')
    pass = false
    for (var i = 0; !pass && i < this.whitelist.length; i++) {
      logger.debug('merge: whitelist loop, index "%d", element "%s"', i, this.whitelist[i])
      var w = this.whitelist[i]
      if (typeof w === 'string') {
        logger.debug('merge: whitelist loop, whitelist element type is "string"')
        pass = w === change.id
      } else {
        logger.debug('merge: whitelist loop, whitelist element type is "%s"', typeof w)
        pass = w.exec(change.id)
      }
    }
    if (!pass) {
      logger.debug('merge: pass, no attachments')
      f._attachments = {}
      logger.debug('Returning from merge with call to fetchAll')
      return this.fetchAll(change, [], [])
    }
  }

  var need = []
  var changed = false
  for (var v in s.versions) {
    logger.debug('merge: skim version "%s"', v)
    var tgz = s.versions[v].dist.tarball
    logger.debug('merge: skim tgz "%s"', tgz)
    var att = path.basename(url.parse(tgz).pathname)
    logger.debug('merge: skim att "%s"', att)
    var ver = s.versions[v]
    f.versions = f.versions || {}

    if (!f.versions[v] || f.versions[v].dist.shasum !== ver.dist.shasum) {
      logger.debug('merge: fat missing skim version or shasum mismatch')
      f.versions[v] = s.versions[v]
      need.push(v)
      changed = true
    } else if (!f._attachments[att]) {
      logger.debug('merge: fat missing attachment for att')
      need.push(v)
      changed = true
    }
  }

  change.log('need', need)

  // remove any versions that s removes, or which lack attachments
  for (var v in f.versions) {
    logger.debug('merge: processing fat versions that skim removes')
    if (!s.versions[v]) {
      logger.debug('merge: deleting fat version "%s"', v)
      change.log('deleting version', v)
      delete f.versions[v]
    }
  }


  for (var a in f._attachments) {
    logger.debug('merge: fat attachment "%s"', a)
    var found = false
    for (var v in f.versions) {
      logger.debug('merge: attachment: fat version "%s"', v)
      var tgz = f.versions[v].dist.tarball
      logger.debug('merge: attachment: fat tgz "%s"', tgz)
      var b = path.basename(url.parse(tgz).pathname)
      logger.debug('merge: attachment: fat b "%s"', b)
      if (b === a) {
        logger.debug('merge: attachment: found existing attachment in version')
        change.log('found existing attachment', b)
        found = true
        break
      }
    }
    if (!found) {
      logger.debug('merge: attachment: version not found, deleting fat attachment')
      change.log('deleting attachment', a)
      delete f._attachments[a]
      changed = true
    }
  }

  logger.debug('merge: sync fat with skim')
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
    logger.debug('merge: nothing changed, calling resume')
    this.resume()
  } else {
    logger.debug('merge: changed, calling fetchAll')
    this.fetchAll(change, need, [])
  }

  logger.debug('Leaving merge')
}

FullFat.prototype.put = function put(change, did) {
  logger.debug('Entered put')
  change.log(arguments.callee.name)
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

  // It's important that we do everything in enumeration order,
  // because couchdb is a jerk, and ignores disposition headers.
  // Still include the filenames, though, so at least we dtrt.
  // did.forEach(function(att) {
  //   logger.debug('put: did begin: att name "%s"', att.name)
  //   atts[att.name] = {
  //     length: att.length,
  //     follows: true
  //   }
  //
  //   if (att.type)
  //     atts[att.name].type = att.type
  //
  //   logger.debug('put: did end: att name "%s"', att.name)
  // })

  var send = []
  Object.keys(atts).forEach(function (name) {
    logger.debug('put: object keys begin: atts name "%s"', JSON.stringify(name))
    var att = atts[name]

    if (att.follows !== true) {
      logger.debug('put: atts: does not follow')
      logger.debug('Returning from put')
      return
    }

    send.push([name, att])
    attSize += att.length

    var b = '\r\n--' + boundary + '\r\n' +
            'content-length: ' + att.length + '\r\n' +
            'content-disposition: attachment; filename=' +
            JSON.stringify(name) + '\r\n'

    if (att.type)
      b += 'content-type: ' + att.type + '\r\n'

    b += '\r\n'

    boundaries.push(b)
    bSize += b.length
    logger.debug('put: object keys end: atts name "%s"', JSON.stringify(name))
  })

  // one last boundary at the end
  var b = '\r\n--' + boundary + '--'
  bSize += b.length
  boundaries.push(b)

  // put with new_edits=false to retain the same rev
  // this assumes that NOTHING else is writing to this database!
  var p = url.parse(this.fat + '/' + encodeURIComponent(f.name) + '?new_edits=false')
  logger.debug('put: url "%s"', p.pathname)
  p.method = 'PUT'
  p.headers = {
    'user-agent': this.ua,
    'content-type': 'multipart/related;boundary="' + boundary + '"',
    'connection': 'close'
  }

  var doc = new Buffer(JSON.stringify(f), 'utf8')
  var len = 0

  // now, for the document
  var b = '--' + boundary + '\r\n' +
          'content-type: application/json\r\n' +
          'content-length: ' + doc.length + '\r\n\r\n'
  bSize += b.length

  p.headers['content-length'] = attSize + bSize + doc.length

  logger.debug('put: executing put request')
  var req = hh.request(p)
  logger.debug('put: setting request error event')
  req.on('error', this.emit.bind(this, 'error'))
  logger.debug('put: writing boundary')
  req.write(b, 'ascii')
  logger.debug('put: writing doc')
  change.log('writing doc:', JSON.stringify(JSON.parse(doc), null, 2))
  req.write(doc)
  logger.debug('put: calling putAttachments')
  this.putAttachments(req, change, boundaries, send)
  logger.debug('put: setting request response event with onputres')
  req.on('response', parse(this.onputres.bind(this, change)))
  logger.debug('Leaving put')
}

FullFat.prototype.putAttachments = function putAttachments(req, change, boundaries, send) {
  logger.debug('Entered putAttachments')
  // send is the ordered list of [[name, attachment object],...]
  var b = boundaries.shift()
  var ns = send.shift()

  // last one!
  if (!ns) {
    logger.debug('putAttachments: no ns, writing boundary')
    change.log(arguments.callee.name, '---last')
    req.write(b, 'ascii')
    logger.debug('Returning from putAttachments with request end()')
    return req.end()
  }

  var name = ns[0]
  logger.debug('putAttachments: name "%s"', name)
  logger.debug('putAttachments: writing boundary')
  change.log(arguments.callee.name, name, 'start')
  req.write(b, 'ascii')
  var file = path.join(this.tmp, change.id + '-' + change.seq, name)
  logger.debug('putAttachments: file "%s"', file)
  logger.debug('putAttachments: creating read stream for file "%s"', file)
  var data = fs.readFileSync(file)
  //var fstr = fs.createReadStream(file)

  logger.debug('putAttachments: setting file stream end event')
  // fstr.on('end', function() {
  req.write(data, function() {
    logger.debug('putAttachments: end event: emit upload for "%s"', name)
    change.log(arguments.callee.name, name, 'done')
    this.emit('upload', {
      change: change,
      name: name
    })
    logger.debug('putAttachments: end event: calling putAttachments')
    this.putAttachments(req, change, boundaries, send)
  }.bind(this))

  // logger.debug('putAttachments: setting file stream error event')
  // fstr.on('error', this.emit.bind(this, 'error'))
  // fstr.pipe(req, { end: false })
  logger.debug('Leaving putAttachments')
}

FullFat.prototype.onputres = function onputres(change, er, data, res) {
  logger.debug('Entered onputres')
  change.log(arguments.callee.name)

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
    logger.debug('onputres: missing stub error, calling onfatget with status code 404')
    change.didFake404 = true
    this.onfatget(change, { statusCode: 404 }, {}, {})
  } else if (er) {
    logger.debug('onputres: emit error')
    this.emit('error', er)
  } else {
    logger.debug('onputres: emit put')
    this.emit('put', change, data)
    // Just a best-effort cleanup.  No big deal, really.
    logger.debug('onputres: calling rimraf for cleanup')
    rimraf(this.tmp + '/' + change.id + '-' + change.seq, function() {})
    logger.debug('onputres: calling resume')
    this.resume()
  }
  logger.debug('Leaving onputres')
}

FullFat.prototype.fetchAll = function fetchAll(change, need, did) {
  logger.debug('Entered fetchAll')
  change.log(arguments.callee.name)
  var f = change.fat
  logger.debug('fetchAll: tmp component this.tmp: "%s"', this.tmp)
  logger.debug('fetchAll: tmp component change.id: "%s"', change.id)
  logger.debug('fetchAll: tmp component change.seq: "%d"', change.seq)
  var tmp = path.resolve(this.tmp, change.id + '-' + change.seq)
  var len = need.length
  if (!len) {
    logger.debug('fetchAll: Returning from fetchAll, nothing to fetch')
    return this.put(change, did)
  }

  var errState = null

  logger.debug('fetchAll: creating tmp dir: "%s"', tmp)
  mkdirp(tmp, function(er) {
    if (er) {
      logger.debug('Returning from fetchAll on error to create tmp dir')
      return this.emit('error', er)
    }
    logger.debug('fetchAll: Binding fetchOne for each item in need')
    need.forEach(this.fetchOne.bind(this, change, need, did))
  }.bind(this))
  logger.debug('Leaving fetchAll')
}

FullFat.prototype.fetchOne = function fetchOne(change, need, did, v) {
  logger.debug('Entered fetchOne')
  change.log(arguments.callee.name)
  var f = change.fat
  logger.debug('fetchOne: version (v): "%s"', v)
  var r = url.parse(change.doc.versions[v].dist.tarball)
  logger.debug('fetchOne: url based on skim doc (r): "%s"', r.pathname)
  if (this.registry) {
    logger.debug('fetchOne: url based on registry')
    var p = '/' + encodeURIComponent(change.id) + '/-/' + path.basename(r.pathname)
    logger.debug('fetchOne: registry: path (p): "%s"', p)
    r = url.parse(this.registry + p)
    logger.debug('fetchOne: url based on registry (r): "%s"', r)
  }

  r.method = 'GET'
  r.headers = {
    'user-agent': this.ua,
    'connection': 'close'
  }

  logger.debug('fetchOne: executing request')
  var req = hh.request(r)
  logger.debug('fetchOne: setting request error event')
  req.on('error', this.emit.bind(this, 'error'))
  logger.debug('fetchOne: setting request response event to call onattres')
  req.on('response', this.onattres.bind(this, change, need, did, v, r))
  req.end()
  logger.debug('Leaving fetchOne')
}

FullFat.prototype.onattres = function onattres(change, need, did, v, r, res) {
  logger.debug('Entered onattres')
  change.log(arguments.callee.name)
  var f = change.fat
  var att = r.href
  logger.debug('onattres: att: "%s"', att)
  var sum = f.versions[v].dist.shasum
  logger.debug('onattres: fat shasum: "%s"', sum)
  var filename = path.basename(f.name) + '-' + v + '.tgz'
  logger.debug('onattres: fat filename: "%s"', filename)
  var file = path.join(this.tmp, change.id + '-' + change.seq, filename)
  logger.debug('onattres: fat file: "%s"', file)

  // TODO: If the file already exists, get its size.
  // If the size matches content-length, get the md5
  // If the md5 matches content-md5, then don't bother downloading!

  function skip() {
    logger.debug('onattres: entered skip')
    rimraf(file, function() {})
    delete f.versions[v]
    if (f._attachments)
      delete f._attachments[file]
    need.splice(need.indexOf(v), 1)
    maybeDone(null)
    logger.debug('onattres: leaving skip')
  }

  var maybeDone = function maybeDone(a) {
    logger.debug('onattres: entered maybeDone')
    if (a)
      this.emit('download', a)
    if (need.length === did.length)
      this.put(change, did)
    logger.debug('onattres: leaving maybeDone')
  }.bind(this)

  // if the attachment can't be found, then skip that version
  // it's uninstallable as of right now, and may or may not get
  // fixed in a future update
  if (res.statusCode !== 200) {
    logger.debug('onattres: attachment not found, status code "%d"', res.statusCode)
    var er = new Error('Error fetching attachment: ' + att)
    er.statusCode = res.statusCode
    er.code = 'attachment-fetch-fail'
    if (this.missingLog) {
      logger.debug('Returning from onattres with an append to missing log')
      return fs.appendFile(this.missingLog, att + '\n', skip)
    } else {
      logger.debug('Returning from onattres with an error')
      return this.emit('error', er)
    }
  }

  var fstr = fs.createWriteStream(file)

  // check the shasum while we're at it
  var sha = crypto.createHash('sha1')
  var shaOk = false
  var errState = null

  sha.on('data', function(c) {
    logger.debug('onattres: entered sha on data')
    c = c.toString('hex')
    if (c === sum)
      shaOk = true
    logger.debug('onattres: leaving sha event')
  }.bind(this))

  if (!res.headers['content-length']) {
    var counter = new Counter()
    res.pipe(counter)
  }

  res.pipe(sha)
  res.pipe(fstr)

  fstr.on('error', function(er) {
    logger.debug('onattres: entered fstr on error')
    er.change = change
    er.version = v
    er.path = file
    er.url = att
    this.emit('error', errState = errState || er)
    logger.debug('onattres: leaving fstr event')
  }.bind(this))

  fstr.on('close', function() {
    logger.debug('onattres: entered fstr on close')
    if (errState || !shaOk) {
      // something didn't work, but the error was squashed
      // take that as a signal to just delete this version
      logger.debug('onattres: returning from fstr close event with skip()')
      return skip()
    }
    // it worked!  change the dist.tarball url to point to the
    // registry where this is being stored.  It'll be rewritten by
    // the _show/pkg function when going through the rewrites, anyway,
    // but this url will work if the couch itself is accessible.
  var filename = path.basename(f.name) + '-' + v + '.tgz'
    var newatt = this.publicFat + '/' + encodeURIComponent(change.id) +
                 '/' + path.basename(change.id) + '-' + v + '.tgz'
    logger.debug('onattres: fstr close event: newatt "%s"', newatt)
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
    logger.debug('onattres: fstr close event: calling did.push')
    did.push(a)
    logger.debug('onattres: fstr close event: calling maybeDone')
    maybeDone(a)

    logger.debug('onattres: leaving fstr close event')
  }.bind(this))
  logger.debug('Leaving onattres')
}

FullFat.prototype.destroy = function() {
  logger.debug('Entered destroy')
  if (this.follow)
    this.follow.die()
  logger.debug('Leaving destroy')
}

FullFat.prototype.pause = function() {
  logger.debug('Entered pause')
  if (this.follow) {
    logger.debug('pause: pausing follow')
    this.follow.pause()
  }
  logger.debug('Leaving pause')
}

FullFat.prototype.resume = function() {
  logger.debug('Entered resume')
  this.writeSeq()
  if (this.follow) {
    logger.debug('resume: resume follow')
    this.follow.resume()
  }
  logger.debug('Leaving resume')
}

util.inherits(Counter, stream.Writable)
function Counter(options) {
  logger.debug('Entered Counter')
  stream.Writable.call(this, options)
  this.count = 0
  logger.debug('Leaving Counter')
}
Counter.prototype._write = function(chunk, encoding, cb) {
  logger.debug('Entered Counter _write')
  this.count += chunk.length
  cb()
  logger.debug('Leaving Counter _write')
}
