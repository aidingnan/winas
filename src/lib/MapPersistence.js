/**
 * A Key-Value Persistent Store
 * key - hash/uuid
 * value - Array of json object or string
 * persietent file file is named with the first four bytes of the hash/uuid
 * File names with the same prefix store in same p-file
 * auto create **sorted-cache** by given compare func
 * use LRU policy for memory cache
 */

const fs = require('fs')
const path = require('path')

const uuid = require('uuid')
const Promise = require('bluebird')

const DEFAULT_CACHE_SIZE = 500
const DEFAULT_PREFIX_LENGTH = 4
/**
 * @param dir - work directory
 * @param tmp - tmp directory
 * @param opts - work options
 * @param opts.cacheSize - memory cache size default 500
 * @param opts.valueType - value type default json
 * @param opts.prefixLength - prefix length
 * @param opts.sortFunc - sort founction
 * @param opts.dataFunc - generate new data / vaild data
 */
class MapPersistent {
  constructor(dir, tmp, opts = {}) {

    this.workdir = dir
    this.tmpdir = tmp
    this.cacheSize = opts.cacheSize > 0 ? opts.cacheSize : DEFAULT_CACHE_SIZE
    this.prefixLength = opts.prefixLength > 0 ? opts.prefixLength : DEFAULT_PREFIX_LENGTH
    this.sortFunc = opts.sortFunc
    this.dataFunc = opts.dataFunc
    // limited cache
    this.cacheA = []
    // use map for quick search
    this.cacheM = new Map()
    // use Map to lock CUD operations on same file
    this.lock = new Map()
  }

  cacheAdd(k, v) {
    let idx = this.cacheA.findIndex(x => x === k)
    if (idx !== -1) { // move to last
      this.cacheA.splice(idx, 1)
      this.cacheA.push(k)
      return
    }
    // clean cache
    if (this.cacheA.length >= this.cacheSize) {
      let tmp = this.cacheA.slice(0, this.cacheA.length - this.cacheSize + 1)
      tmp.forEach(x => this.cacheM.delete(x))
    }
    this.cacheA.push(k)
    this.cacheM.set(k, v)
  }

  // remove cache
  cacheRemove(k) {
    let idx = this.cacheA.findIndex(x => x === k)
    if (idx === -1) return
    this.cacheA.splice(idx, 1)
    this.cacheM.delete(k)
  }

  // lock same CRUD path
  call(command, { k, v }, callback) {
    let lockKey = k.slice(0, this.prefixLength)
    let cb = (...args) => {
      let ops = this.lock.get(lockKey)
      ops.shift() // clean the first job in queue (current job)
      if (ops.length) this.schedule(lockKey)
      else {
        this.lock.delete(lockKey)
      }
      process.nextTick(() => callback(...args))
    }

    if (this.lock.has(lockKey)) {
      this.lock.get(lockKey).push({ command, props: {k, v}, cb })
    } else {
      this.lock.set(lockKey, [{ command, props: {k, v}, cb }])
      this.schedule(lockKey)
    }
  }

  schedule(key) {
    let ops = this.lock.get(key)
    if (!ops || !ops.length) throw new Error('lock error')
    let { command, props, cb } = ops[0] // do the first job in queue
    this[command](props, cb)
  }

  _read(k, callback) {
    let filePath = path.join(this.workdir, k.slice(0, this.prefixLength))
    fs.readFile(filePath, (err, data) => {
      if (err && err.code !== 'ENOENT') {
        return callback(err)
      }
      let newdata = []
      if (data) {
        try {
          data = JSON.parse(data)
        } catch(e) {
          return callback(e)
        }
        newdata = data
      }
      callback(null, newdata)
    })
  }

  set({ k, v }, callback) {
    if (!k || typeof k !== 'string' || k.length < this.prefixLength)
      return process.nextTick(() => callback(new Error('illegal k:' + k)))
    this.call('_set', { k, v }, callback)
  }

  async setAsync (props) {
    return Promise.promisify(this.set).bind(this)(props)
  }

  _set({ k, v }, callback) {
    this._read(k, (err, data) => {
      if (err) return callback(err)
      let d = data.find(x => x.k === k)
      if (d) return callback(null, null)
      //TODO: use binary search to find insert index
      data.push({ k, v })
      data.sort(this.sortFunc)
      let tmp = path.join(this.tmpdir, uuid.v4())
      fs.writeFile(tmp, JSON.stringify(data), err => err ? callback(err)
        : fs.rename(tmp, path.join(this.workdir, k.slice(0, this.prefixLength)), err => err ? callback(err)
        : (this.cacheAdd(k, v), callback(null, null))))
    })
  }

  update({ k, v }, callback) {
    if (!k || typeof k !== 'string' || k.length < this.prefixLength)
      return process.nextTick(() => callback(new Error('illegal k:' + k)))
    this.call('_update', { k, v }, callback)
  }

  async updateAsync (props) {
    return Promise.promisify(this.update).bind(this)(props)
  }

  _update({ k, v }, callback) {
    this._read(k, (err, data) => {
      if (err) return callback(err)
      let d = data.find(x => x.k === k)
      if (!d) return callback(new Error(`${k} not found`))
      d.v = Object.assign({}, d.v, v)
      let tmp = path.join(this.tmpdir, uuid.v4())
      fs.writeFile(tmp, JSON.stringify(data), err => err ? callback(err)
        : fs.rename(tmp, path.join(this.workdir, k.slice(0, this.prefixLength)), err => err ? callback(err)
        : (this.cacheAdd(k, v), callback(null, null))))
    })
  }

  delete({ k }, callback) {
    if (!k || typeof k !== 'string' || k.length < this.prefixLength)
      return process.nextTick(() => callback(new Error('illegal k:' + k)))
    this.call('_delete', { k }, callback)
  }

  async deleteAsync (props) {
    return Promise.promisify(this.delete).bind(this)(props)
  }

  _delete({ k }, callback) {
    this._read(k, (err, data) => {
      if (err) return callback(err)
      let index = data.findIndex(x => x.k === k)
      if (index == -1) return callback(null, null)
      data.splice(index, 1)
      let tmp = path.join(this.tmpdir, uuid.v4())
      fs.writeFile(tmp, JSON.stringify(data), err => err ? callback(err)
        : fs.rename(tmp, path.join(this.workdir, k.slice(0, this.prefixLength)), err => err ? callback(err)
        : (this.cacheRemove(k), callback(null, null))))
    })
  }

  get ({ k }, callback) {
    if (!k || typeof k !== 'string' || k.length < this.prefixLength)
      return process.nextTick(() => callback(new Error('illegal k:' + k)))
    if (this.cacheM.has(k)) {
      let v = this.cacheM.get(k)
      return process.nextTick(() => callback(null, v))
    }
    this.call('_get', { k }, callback)
  }

  async getAsync (props) {
    return Promise.promisify(this.get).bind(this)(props)
  }

  // TODO: read without lock
  _get({ k }, callback) {
    this._read(k, (err, data) => {
      if (err) return callback(err)
      let d = data.find(x => x.k === k)
      if (d) this.cacheAdd(k, d)
      return callback(null, d)
    })
  }
}

module.exports= MapPersistent