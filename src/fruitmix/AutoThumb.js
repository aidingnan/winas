const fs = require('fs')
const path = require('path')
const UUID = require('uuid')
const readline = require('readline')
const child = require('child_process')
const EventEmitter = require('events')
const crypto = require('crypto')

const mkdirp = require('mkdirp')
const rimraf = require('rimraf')

const debug = require('debug')('thumbnail')

const { isSHA256, isNonNullObject, isNormalizedAbsolutePath } = require('../lib/assertion')

const ERROR = (code, _text) => text => Object.assign(new Error(text || _text), { code })

const EFAIL = ERROR('EFAIL', 'operation failed')
const EINVAL = ERROR('EINVAL', 'invalid argument')
const EINTR = ERROR('EINTR', 'operation interrupted')
const ENOENT = ERROR('ENOENT', 'entry not found')

//
// courtesy https://stackoverflow.com/questions/5467129/sort-javascript-object-by-key
// for letting me know the comma operator
const sortObject = o => Object.keys(o).sort().reduce((r, k) => (r[k] = o[k], r), {})

// parse query to opts
const parseQuery = query => {
  let { width, height, modifier, autoOrient, colors } = query

  if (width !== undefined) {
    width = parseInt(width)
    if (!Number.isInteger(width) || width === 0 || width > 4096) return EINVAL('invalid width')
  }

  if (height !== undefined) {
    height = parseInt(height)
    if (!Number.isInteger(height) || height === 0 || height > 4096) return EINVAL('invalid height')
  }

  if (colors !== undefined) {
    colors = parseInt(colors)
    if (!Number.isInteger(colors) || colors === 0 || colors > 4096) return EINVAL('invalid colors')
  }

  if (!width && !height) return EINVAL('no geometry')

  if (!width || !height) modifier = undefined
  if (modifier && modifier !== 'caret') return EINVAL('unknown modifier')
  if (autoOrient !== undefined) {
    if (autoOrient !== 'true') return EINVAL('invalid autoOrient')
    autoOrient = true
  }

  return { width, height, modifier, autoOrient, colors }
}

// hash stringified option object
const genKey = (fingerprint, opts) => fingerprint +
  crypto.createHash('sha256').update(JSON.stringify(sortObject(opts))).digest('hex')
// generate geometry string for convert
const geometry = (width, height, modifier) => {
  let str

  if (!height) {
    str = `${width.toString()}`
  } else if (!width) {
    str = `x${height.toString()}`
  } else {
    str = `${width.toString()}x${height.toString()}`

    switch (modifier) {
      case 'caret':
        str += '^'
        break
      default:
        break
    }
  }
  return str
}

// generate convert args
const genArgs = (src, tmp, opts, type) => {
  if (type === 'JPEG') type = 'JPG'
  let ext = path.extname(src).slice(1).toUpperCase()

  if (ext !== type) {
    src = `${type.toLowerCase()}:${src}`
  }

  let args = []
  args.push(src + '[0]')
  if (opts.autoOrient) args.push('-auto-orient')
  if (opts.colors) {
    args.push('-colors')
    args.push(opts.colors)
  }
  args.push('-thumbnail')
  args.push(geometry(opts.width, opts.height, opts.modifier))
  args.push('-background')
  args.push('white')
  args.push('-alpha')
  args.push('background')
  args.push(tmp)
  return args
}

// spawn a command, err race
const spawn = (cmd, args, callback) => {
  let spawn = child.spawn(cmd, args)
  spawn.on('error', err => {
    spawn.removeAllListeners()
    callback(err)
  })

  spawn.on('exit', (code, signal) => {
    spawn.removeAllListeners()
    if (signal) {
      callback(new Error(`exit signal ${signal}`))
    } else if (code) {
      callback(new Error(`exit code ${code}`))
    } else {
      callback()
    }
  })
}



const SEPLIT_SYMBOL = '.auto.thumb.'

const video = ['RM', 'RMVB', 'WMV', 'AVI', 'MPEG', 'MP4', '3GP', 'MOV', 'FLV', 'MKV']

const vaildLine = (l) => {
  if (!l || !l.length) return false
  const ls = l.split(SEPLIT_SYMBOL)
  if (ls.length != 3 || !ls.every(x => x.length)) return false
  try {
    const metadata = JSON.parse(ls[1])
    if (!isNonNullObject(metadata)) return false
  } catch (e) {
    return false
  }
  if (!isSHA256(ls[2])) return false
  return true
}

/*
 * Worker - create thumbnail
 */
class Worker extends EventEmitter {
  constructor(fpath, sha256, metadata, thumbDir, query, tmpDir) {
    super()
    this.file = fpath
    this.sha256 = sha256
    this.metadata = metadata
    this.thumbDir = thumbDir
    this.tmpDir = tmpDir
    this.query = query
    this.work()
  }

  work() {
    this.working = true
    fs.lstat(this.file, (err, stats) => {
      if (err) return this._finish(err)
      if (!stats.isFile()) return this._finish(new Error('not file'))
      this.convert(this._finish.bind(this))
    })
  }

  convert(callback) {
    const isVideo = video.includes(this.metadata.type)
    if (isVideo) this.query.autoOrient = undefined
    const opts = parseQuery(this.query)
    const key = genKey(this.sha256, opts)
    const tp = path.join(this.thumbDir, key)
    fs.lstat(tp, err => {
      if (err && err.code === 'ENOENT') {
        isVideo ? this._convertVideo(opts, key, callback)
          : this._convert(opts, key, callback)
      } else {
        process.nextTick(() => callback(null, tp))
      }
    })
  }

  _convertVideo(opts, key, callback) {
    const type = this.metadata.type
    const tp = path.join(this.thumbDir, key)
    const tmp = path.join(this.tmpDir, UUID.v4() + '.jpg')
    const tmp1 = path.join(this.tmpDir, UUID.v4() + '.mp4')
    const pathv = tp + '-v'

    const origWidth = this.metadata.w
    const origHeight = this.metadata.h
    const expectedWidth = opts.width
    const expectedHeight = opts.height

    // 1. keep aspect ratio
    // 2. avoid up scale
    // 3. no crop (client side crop)
    let width, height, scale
    if (expectedWidth >= origWidth && expectedHeight >= origHeight) {
      width = origWidth
      height = origHeight
    } else if (opts.caret) {  // fill expected rectangle
      if (Math.floor(expectedWidth / origWidth * origHeight) >= expectedHeight) {
        scale = `scale=${expectedWidth}:-2`
      } else {
        scale = `scale=-2:${expectedHeight}`
      }
    } else {  // contained in expected rectangle
      if (Math.floor(expectedWidth / origWidth * origHeight) <= expectedHeight) {
        scale = `scale=${expectedWidth}:-2`
      } else {
        scale = `scale=-2:${expectedHeight}`
      }
    }

    /* ffmpeg -loglevel quiet -y -t 15 -i rmvb-sample01.rmvb -an -vf scale=200:200 thumb.mp4 */
    const tmp2 = path.join(this.tmpDir, UUID.v4() + '.jpg')
    let args = genArgs(pathv, tmp2, opts, type)
    let _args = ['-limit', 'memory', '16MiB', '-limit', 'map', '32MiB', ...args]

    spawn('ffmpeg', [
      '-loglevel', 'quiet', '-y', '-t', '15', '-i', this.file,
      '-an', '-vf', scale, tmp1
    ], err => err ? callback(err)
      : fs.rename(tmp1, pathv, err => err ? callback(err)
        : spawn('convert', _args, err => err ? callback(err)
          : fs.rename(tmp2, tp, err => err ? callback(err)
            : callback(null, tp)))))
  }

  _convert(opts, key, callback) {
    const type = this.metadata.type
    const tp = path.join(this.thumbDir, key)
    const tmp = path.join(this.tmpDir, UUID.v4() + '.jpg')
    let args = genArgs(this.file, tmp, opts, type)
    let _args = ['-limit', 'memory', '16MiB', '-limit', 'map', '32MiB', ...args]
    spawn('convert', _args, err => err ? callback(err)
      : fs.rename(tmp, tp, err => err ? callback(err)
        : callback(null, tp)))
  }

  _finish(err, data) {
    this.working = false
    this.finished = true
    this.emit('finish', err, data)
  }
}

class AutoThumb extends EventEmitter {
  constructor(thumbDir, tmpDir, query) {
    super()
    this.thumbDir = thumbDir
    this.tmp = tmpDir
    this.query = query
    this.tasksP = path.join(this.tmp, UUID.v4())
    this.tasksStream = fs.createWriteStream(this.tasksP)
    this.tasksStream.on('error', this.onTaskStreamError.bind(this))
    this.tasksStream.on('close', this.onTaskStreamError.bind(this))
    this.fseek = 0
    this.working = []
    this.workerCount = 1
  }

  onTaskStreamError(...args) { // renew
    this.tasksStream = fs.createWriteStream(this.tasksP, { flags: 'a+' })
    this.tasksStream.write('\n')
    this.tasksStream.on('error', this.onTaskStreamError.bind(this))
    this.tasksStream.on('close', this.onTaskStreamError.bind(this))
  }

  req(fpath, metadata, hash) {
    let task = fpath + SEPLIT_SYMBOL + JSON.stringify(metadata) + SEPLIT_SYMBOL + hash + '\n'
    this.tasksStream.write(task, () => {
      this.schedule()
    })
  }

  schedule() {
    if (this.scheduling) return
    if (this.destroyed) return
    this.scheduling = true
    const exit = closed => {
      if (!closed) {
        rl.removeAllListeners()
        rl.on('close', () => { })
        rl.close()
      }
      this.scheduling = false
    }

    const rl = readline.createInterface({
      input: fs.createReadStream(this.tasksP, { start: this.fseek })
    })
    rl.on('line', line => {
      if (this.destroyed) return exit()
      if (this.working.length >= this.workerCount) return exit()
      let l = line.toString()
      this.fseek += (l.length + 1) // +1 for \n
      if (!vaildLine(l)) return  console.log('pass line', l)// bypass
      const ls = l.split(SEPLIT_SYMBOL)
      const fp = ls[0], metadata = JSON.parse(ls[1]), sha256 = ls[2]
      const worker = new Worker(fp, sha256, metadata, this.thumbDir, this.query, this.tmp)
      worker.on('finish', (err, fp) => {
        const index = this.working.findIndex(x => x === worker)
        if (index !== -1) {
          this.working = [...this.working.slice(0, index),
          ...this.working.slice(index + 1)]
        }
        this.emit('workFinished', sha256, err, fp)
        this.schedule()
      })
      this.working.push(worker)
    })
    rl.on('close', () => exit(true))
  }

  destroy() {
    this.tasksStream.removeAllListeners()
    this.tasksStream.on('error', () => {})
    this.tasksStream.end()
    this.destroyed = true
  }
}

module.exports = AutoThumb