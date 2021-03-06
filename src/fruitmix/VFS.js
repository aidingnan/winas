const Promise = require('bluebird')
const path = require('path')
const fs = Promise.promisifyAll(require('fs'))
const EventEmitter = require('events')
const crypto = require('crypto')
const child = require('child_process')

const mkdirp = require('mkdirp')
const mkdirpAsync = Promise.promisify(mkdirp)
const rimraf = require('rimraf')
const rimrafAsync = Promise.promisify(rimraf)
const UUID = require('uuid')
const deepFreeze = require('deep-freeze')

const E = require('../lib/error')
const Magic = require('../lib/magic')

const log = require('winston')
const sanitize = require('sanitize-filename')
const xattr = require('fs-xattr')       // TODO remove
const { saveObjectAsync } = require('../lib/utils')
const autoname = require('../lib/autoname')
const { isUUID, isSHA256, isNonEmptyString } = require('../lib/assertion')


const Node = require('./vfs/node')
const File = require('./vfs/file')
const Directory = require('./vfs/directory')
const Backup = require('./backup/backup')
const AutoThumb = require('./AutoThumb')

const { btrfsConcat, btrfsClone, btrfsClone2 } = require('../lib/btrfs')

const { readXstat, forceXstat, updateFileTags, assertDirXstatSync, assertFileXstatSync } = require('../lib/xstat')

const Debug = require('debug')
const smbDebug = Debug('samba')
const debugi = require('debug')('fruitmix:indexing')

const debug = Debug('vfs')

const Forest = require('./vfs/forest')
const { mkdir, mkfile, mvdir, mvfile, clone, send } = require('./vfs/underlying')

// TODO move to lib
const Throw = (err, code, status) => {
  err.code = code
  err.status = status
  throw err
}

const EINVAL = err => { throw Object.assign(err, 'EINVAL', 400) }
const EINCONSISTENCE = err => { throw Object.assign(err, 'EINCONSISTENCE', 503) }

/**
VFS is the core module encapsulating all virtual file system operations.

It provides three interfaces:
1. file system interface for retrieving file system information and file operations, servicing Upload  module
2. xcopy interface for copy or move files around, servicing XCopy module. 


VFS observes/reacts to User and Drive module, which is conceptually equivalent to value props in React.

VFS requires the following modules:

1. Forest, internal module for indexing
2. MediaMap, injected, which is synchronously coupled with Forest
3. Xstat, injected, which is a stateful lib
4. Underlying, internal module for operation

@module VFS
*/

/**
Policy type is used for resolving name conflict when operating on files and directories.

@typedef Policy
@type {array}
@property {string} 0 - same policy
@property {string} 1 - diff policy
*/

/**

*/
class VFS extends EventEmitter {

  /**
  Create a VFS module

  @param {object} opts
  @param {string} opts.fruitmixDir - fruitmix root directory
  @param {MediaMap} opts.mediaMap - mediamap module
  @param {User} user - user module
  @param {Drive} drive - drive module
  */
  constructor (opts, user, drive, tag) {
    super()

    this.fruitmixDir = opts.fruitmixDir
    this.tmpDir = path.join(this.fruitmixDir, 'tmp')
    this.driveDir = path.join(this.fruitmixDir, 'drives')
    mkdirp.sync(this.tmpDir)

    //backup add
    global.TMPDIR = () => this.tmpDir

    this.mediaMap = opts.mediaMap

    // observer user
    this.user = user
    Object.defineProperty(this, 'users', { get () { return this.user.users } })
    this.user.on('Update', () => this.handleUserDriveUpdate())

    // observe drive
    this.drive = drive
    Object.defineProperty(this, 'drives', { get () { return this.drive.drives } })
    this.drive.on('Update', () => this.handleUserDriveUpdate())

    this.tag = tag
    Object.defineProperty(this, 'tags', { get () { return this.tag.tags } })
    
    this.autoThumb = new AutoThumb(path.join(this.fruitmixDir, 'thumbnail'), this.tmpDir, {
      width: 1080,
      height: 1080,
      autoOrient: 'true',
      modifier: 'caret'
    })

    this.forest = new Forest(this.fruitmixDir, opts.mediaMap, this.autoThumb)
    this.metaMap = this.forest.metaMap
    this.timeMap = this.forest.timeMap

    this.backup = new Backup(this)
  }

  /**
  React to user and drive change, update forest.roots accordingly

  TODO doc fires
  */
  handleUserDriveUpdate () {
    let users = this.users || []
    let drives = this.drives || []

    // figure out valid drive
    let valids = drives.filter(drv => {
      if (drv.privacy === true || drv.type === 'backup') {
        let owner = users.find(u => u.uuid === drv.owner)
        if (!owner) return false
        return true
      } else if (drv.privacy === false ) {
        return true 
      } else {
        return false
      }
    }) 

    let toBeRemoved = valids.filter(drv => {
      if ((drv.privacy === true || drv.type === 'backup') && drv.isDeleted) {
        let owner = users.find(u => u.uuid === drv.owner) 
        if (drv.privacy === true && (!owner || owner.status !== this.user.USER_STATUS.DELETED)) return false
        return true
      }
      if ((drv.privacy === false) && drv.isDeleted) return true
      return false
    }).map(drv => drv.uuid)

    // all valid drive uuids that are not root
    let toBeCreated = valids
      .filter(d => !d.isDeleted)
      .map(d => d.uuid)
      .filter(uuid => !this.forest.roots.has(uuid))

    // all root uuids that are not in valids
    let toBeDeleted = Array.from(this.forest.roots.keys())
      .filter(uuid => !valids.find(d => d.uuid === uuid))
    
    if (toBeCreated.length === 0 && toBeDeleted.length === 0 && toBeRemoved.length === 0) return
    let oldKeys = Array.from(this.forest.roots.keys())
    toBeDeleted.forEach(uuid => this.forest.deleteRoot(uuid))

    // report drive
    if (toBeRemoved.length) toBeRemoved.forEach(uuid => debug(`drive:${uuid} be removed;  remove success: ${ this.removeRoot(uuid) }`))

    if (!toBeCreated.length) return this.emit('ForestUpdate', Array.from(this.forest.roots.keys()))

    toBeCreated.forEach(uuid => this.createRoot(uuid))
    this.emit('ForestUpdate', Array.from(this.forest.roots.keys()), oldKeys)
  }

  createRoot(uuid) {
    let dirPath = path.join(this.driveDir, uuid)
    let stats, attr = { uuid }
    try {
      mkdirp.sync(dirPath)
      stats = fs.lstatSync(dirPath)
      // this is tricky but works
      xattr.setSync(dirPath, 'user.fruitmix', JSON.stringify(attr))
    } catch (e) {
      console.log(e)
      return
    }
    let name = path.basename(dirPath)
    let xstat = {
      uuid: attr.uuid,
      type: 'directory',
      name,
      mtime: stats.mtime.getTime()
    }
    let drv = this.drives.find(d => d.uuid === uuid)
    return this.forest.createRoot(uuid, xstat, drv && drv.type === 'backup')
  }

  removeRoot (uuid) {
    let dirPath = path.join(this.driveDir, uuid)
    let success
    try {
      rimraf.sync(dirPath)
      success = true
      this.forest.deleteRoot(uuid)
    } catch (e) {
      console.log(e)
      success = false
    }
    return success
  }

  userCanWriteDrive (user, drive) {
    if (drive.privacy === true || drive.type === 'backup') {
      return user.uuid === drive.owner
    } else if (drive.privacy === false) {
      if (Array.isArray(drive.writelist)) {
        return drive.writelist.includes(user.uuid)
      } else {
        return true
      }
    } else {
      return false
    }
  }

  userCanWriteDir (user, dir) {

    if (user === undefined || dir === undefined) {
      console.log(new Error())
    }

    let drive = this.drives.find(drv => drv.uuid === dir.root().uuid)
    return drive && this.userCanWriteDrive(user, drive)
  }

  isBackupDrive(driveUUID) {
    let index = this.drives.findIndex(x => x.uuid === driveUUID)
    if (index === -1) return false
    let drv = this.drives[index]
    return drv.type === 'backup'
  }

  TMPFILE () {
    return path.join(this.tmpDir, UUID.v4())
  }

  /**
  Try to read the dir with given dir uuid. No permission check.

  This is a best-effort function. It may be used in api layer when error is encountered.
  */
  tryDirRead (dirUUID, callback) {
    let dir = this.forest.uuidMap.get(dirUUID)
    if (dir) {
      dir.read(callback)
    } else {
      let err = new Error('dir not found')
      process.nextTick(() => callback(new Error(err)))
    }
  }

  /**
  @param {object} user - user
  @param {object} props
  @param {object} props.driveUUID
  @param {object} props.dirUUID
  */
  READDIR(user, props, callback) {
    // backup add
    if (this.isBackupDrive(props.driveUUID))
      return this.backup.READDIR(user, props, callback)
    // backup end

    this.dirGET(user, props, (err, combined) => {
      if (err) return callback(err)
      callback(null, combined.entries)
    })
  }

  /**
  @param {object} user - user
  @param {object} props 
  @param {string} [driveUUID] - drive uuid
  @param {string} dirUUID - dir uuid
  @param {string} metadata - true or falsy
  @param {string} counter - true or falsy
  */
  dirGET (user, props, callback) {
    let dir, root, drive

    // find dir
    dir = this.forest.uuidMap.get(props.dirUUID)
    if (!dir) {
      let err = new Error('dir not found')
      err.status = 404
      return process.nextTick(() => callback(err))
    }

    // find root
    root = dir.root()
   
    // find drive 
    drive = this.drives.find(d => d.uuid === root.uuid)

    /**
    If driveUUID is provided, the corresponding drive must contains dir.
    */
    if (props.driveUUID && props.driveUUID !== drive.uuid) {
      let err = new Error('drive does not contain dir')
      err.status = 403
      return process.nextTick(() => callback(err))
    }

    if (!this.userCanWriteDrive(user, drive)) {
      let err = new Error('permission denied') 
      err.status = 403      // TODO 404?
      return process.nextTick(() => callback(err))
    }
     
    // TODO it is possible that dir root is changed during read 
    dir.read((err, entries, whiteout) => {
      if (err) {
        err.status = 500
        callback(err)
      } else {
        let path = dir.nodepath().map(dir => ({
          uuid: dir.uuid,
          name: dir.name,
          mtime: Math.abs(dir.mtime)
        }))
        callback(null, { path, entries })
      }
    })

  }

  /**
  Get a directory (asynchronized with nextTick)

  This function is API, which means it implements http resource model and provides status code.

  without drive:

  - If dir not found, 404
  - If dir.root not accessible, 404

  with drive:

  - if drive is not found, 404
  - if drive is deleted, 404
  - if drive is not accessible, 404 
  - if dir not found, 404 (same as w/o drive)
  - if dir.root not accessible, 404 (same as w/o drive)
  - if dir.root !== drive, 301

  @param {object} user
  @param {object} props
  @param {string} [driveUUID] - drive uuid, if provided the containing relationship is checked
  @param {string} dirUUID - directory uuid
  @returns directory object
  */
  DIR (user, props, callback) {
    let { driveUUID, dirUUID } = props
  
    // specified is the drive specified by driveUUID 
    let specified, dir, drive

    if (driveUUID) {
      specified = this.drives.find(d => d.uuid === driveUUID)
      if (!specified || specified.isDeleted || !this.userCanWriteDrive(user, specified)) {
        let err = new Error('drive not found')
        err.status = 404
        return process.nextTick(() => callback(err))
      }
    }
    
    dir = this.forest.uuidMap.get(props.dirUUID)
    if (!dir) {
      let err = new Error('dir not found')
      err.status = 404
      return callback(err)
    }

    drive = this.drives.find(d => d.uuid === dir.root().uuid)
    if (!drive || drive.isDeleted || !this.userCanWriteDrive(user, drive)) {
      let err = new Error('dir not found') 
      err.status = 404
      return callback(err)
    }

    if (driveUUID) {
      if (drive.uuid !== driveUUID) {
        let err = new Error('dir moved elsewhere')
        err.status = 301
        return callback(err)
      } 
    }

    callback(null, dir) 
  }

  /**
  Make a directory

  @param {object} user
  @param {object} props
  @param {string} [driveUUID] - drive uuid
  @param {string} dirUUID - dir uuid
  @param {string} name - dir name
  @param {Policy} policy - policy to resolve name conflict 
  @parma {boolean} read - true to read dir immediately (for indexing)
  */
  MKDIR (user, props, callback) {
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)
      if (dir.deleted) return callback(Object.assign(new Error('dir not found'), { status: 404 }))
      if (!props.policy) props.policy = [null, null]
      let target = path.join(this.absolutePath(dir), props.name)
      /**
      FIXME: This function is problematic. readXattr may race!
      */
      mkdir(target, props.policy, (err, xstat, resolved) => {
        if (err) return callback(err)
        // this only happens when skip diff policy taking effect
        if (!xstat) return callback(null, null, resolved)
        if (!props.read) return callback(null, xstat, resolved)
        dir.read((err, xstats) => {
          if (err) return callback(err)

          let found = xstats.find(x => x.uuid === xstat.uuid)
          if (!found) {
            let err = new Error(`failed to find newly created directory`)
            err.code = 'ENOENT'
            err.xcode = 'EDIRTY'
            callback(err)
          } else {
            callback(null, found, resolved)
          }
        })
      })
    })
  }

  /**
  Rename a file or directory

  TODO this function should be merged with xcopy version, in future

  @param {object} user
  @param {object} props
  @param {string} props.fromName - fromName
  @param {string} props.toName - toName
  @param {Policy} [props.policy]
  */
  RENAME (user, props, callback) {
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)
      
      let { fromName, toName, policy } = props
      policy = policy || [null, null]
      
      let src = path.join(this.absolutePath(dir), fromName) 
      let dst = path.join(this.absolutePath(dir), toName)
      readXstat(src, (err, srcXstat) => {
        if (err) return callback(err)
        if (srcXstat.type === 'directory') {
          mvdir(src, dst, policy, (err, xstat, resolved) => {
            if (err) return callback(err)
            // race if dir changed FIXME
            // update forest 
            dir.read((err, xstats) => {
              if (err) return callback(err)
              return callback(null, xstat, resolved) 
            })
          })
        } else {
          mvfile(src, dst, policy, (err, xstat, resolved) => {
            if (err) return callback(err)
            // race if dir changed FIXME
            // update forest 
            dir.read((err, xstats) => {
              if (err) return callback(err)
              return callback(null, xstat, resolved) 
            })
          })
        }
      })
    }) 
  }

  /**
  Remove a file or directory

  @param {object} user
  @param {object} props
  @param {string} props.name
  */
  REMOVE (user, props, callback) {
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)
      let { name } = props 
      let target = path.join(this.absolutePath(dir), name)
      rimraf(target, err => callback(err))  
    })
  }

  /**
  NEWFILE create a new file in vfs from a tmp file.

  @param {object} user
  @param {object} props
  @param {string} props.driveUUID - drive uuid
  @param {string} props.dirUUID - dir uuid
  @param {string} props.name - file name
  @param {string} props.data - tmp data file
  @param {number} props.size - file size (not used)
  @param {string} props.sha256 - file hash (fingerprint)
  */
  NEWFILE (user, props, callback) {
    let { name, data, size, sha256, uptype } = props
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)
      if (!props.policy) props.policy = [null, null]
      let target = path.join(this.absolutePath(dir), props.name)
      mkfile(target, props.data, props.sha256 || null, props.policy, (...args) => {
        rimraf(props.data, () => {})
        callback(...args)
      })
    }) 
  }

  /**
  APPEND data after an existing file

  @param {object} user
  @param {object} props
  @param {object} props.name - file name
  @param {object} props.hash - fingerprint of existing file (before appending)
  @param {object} props.data - data file
  @param {object} props.size - data size (not used?)
  @param {object} props.sha256 -data sha256
  */
  APPEND (user, props, callback) {
    let cb = (...args) => {
      if (props.data) {
        rimraf(props.data, () => {})
      }
      return callback(...args)
    }
    this.DIR(user, props, (err, dir) => {
      if (err) return cb(err) 

      let { name, hash, data, size, sha256 } = props

      let target = path.join(this.absolutePath(dir), name)  
      readXstat(target, (err, xstat) => {
        if (err) {
          if (err.code === 'ENOENT' || err.code === 'EISDIR' || err.xcode === 'EUNSUPPORTED') err.status = 403
          return cb(err)
        }

        if (xstat.type !== 'file') {
          let err = new Error('not a file')
          err.code = 'EISDIR'
          err.status = 403
          return cb(err)
        }

        if (xstat.size % (1024 * 1024 * 1024) !== 0) {
          let err = new Error('not a multiple of 1G')
          err.code = 'EALIGN' // kernel use EINVAL for non-alignment of sector size
          err.status = 403
          return cb(err)
        }

        if (xstat.hash !== hash) {
          let err = new Error(`hash mismatch, actual: ${xstat.hash}`)
          err.code = 'EHASHMISMATCH' 
          err.status = 403
          return cb(err)
        }

        let tmp = this.TMPFILE() 

        // concat target and data to a tmp file
        // TODO sync before op
        btrfsConcat(tmp, [target, data], err => {
          if (err) return cb(err)

          // clean tmp data
          rimraf(data, () => {})

          fs.lstat(target, (err, stat) => {
            if (err) return cb(err)
            if (stat.mtime.getTime() !== xstat.mtime) {
              let err = new Error('race detected')
              err.code = 'ERACE'
              err.status = 403
              return cb(err)
            }

            const combineHash = (a, b) => {
              let a1 = typeof a === 'string' ? Buffer.from(a, 'hex') : a
              let b1 = typeof b === 'string' ? Buffer.from(b, 'hex') : b
              let hash = crypto.createHash('sha256')
              hash.update(Buffer.concat([a1, b1]))
              let digest = hash.digest('hex')
              return digest
            }

            // TODO preserve tags
            forceXstat(tmp, { 
              uuid: xstat.uuid, 
              hash: xstat.size === 0 ? sha256 : combineHash(hash, sha256)
            }, (err, xstat2) => {
              if (err) return cb(err)

              // TODO dirty
              xstat2.name = name
              fs.rename(tmp, target, err => err ? cb(err) : cb(null, xstat2))
            })
          })
        })
      })
    })
  }

  /**
  Duplicate a file
  */
  DUP (user, props, callback) {
  }

  /**
  @param {object} user
  @param {object} props
  @param {string} props.driveUUID
  @param {string} props.dirUUID
  @param {string} props.name
  @param {string} props.tags
  */
  ADDTAGS (user, props, callback) {
    try {
      let tags = props.tags
      if (!Array.isArray(tags) || tags.length === 0 || !tags.every(id => Number.isInteger(id) && id >= 0)) 
        throw new Error('invalid tags')

      tags.forEach(id => {
        let tag = this.tags.find(tag => tag.id === id && tag.creator === user.uuid && !tag.deleted)
        if (!tag || tag.creator !== user.uuid) throw new Error(`tag id ${id} not found`)
      })
    } catch (err) {
      err.status = 400
      return process.nextTick(() => callback(err))
    }

    // console.log(user, props, '=================================')
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)

      let tags = Array.from(new Set(props.tags)).sort()   
      let filePath = path.join(this.absolutePath(dir), props.name)   

      readXstat(filePath, (err, xstat) => {
        if (err) return callback(err)
        if (xstat.type !== 'file') {
          let err = new Error('not a file')
          err.code = 'ENOTFILE'
          return callback(err)
        }

        let oldTags = xstat.tags || []
        let newTags = Array.from(new Set([...oldTags, ...tags])).sort()

        if (newTags.length === oldTags.length) {
          callback(null, xstat)
        } else {
          updateFileTags(filePath, xstat.uuid, newTags, callback)
        }
      })
    })  
  }

  REMOVETAGS (user, props, callback) {
    try {
      let tags = props.tags        
      if (!Array.isArray(tags) || tags.length === 0 || !tags.every(id => Number.isInteger(id) && id >= 0))
        throw new Error('invalid tags')

      tags.forEach(id => {
        let tag = this.tags.find(tag => tag.id === id && tag.creator === user.uuid && !tag.deleted)
        if (!tag || tag.creator !== user.uuid) throw new Error(`tag id ${id} not found`)
      })      
    } catch (err) {
      err.status = 400
      return process.nextTick(() => callback(err))
    }

    // normalize
    let tags = Array.from(new Set(props.tags)).sort()
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)

      let filePath = path.join(this.absolutePath(dir), props.name)
      readXstat(filePath, (err, xstat) => {
        if (err) return callback(err)
        if (xstat.type !== 'file') {
          let err = new Error('not a file')
          err.code = 'ENOTFILE'
          return callback(err)
        }

        if (!xstat.tags) return callback(null, xstat) 
        
        // complementary set
        let newTags = xstat.tags.reduce((acc, id) => tags.includes(id) ? acc : [...acc, id], [])
        console.log(newTags)
        updateFileTags(filePath, xstat.uuid, newTags, callback)
      }) 
    })
  }

  // set tags accept empty array
  SETTAGS (user, props, callback) {
    try {
      let tags = props.tags        
      if (!Array.isArray(tags) || tags.length === 0 || !tags.every(id => Number.isInteger(id) && id >= 0))
        throw new Error('invalid tags')

      tags.forEach(id => {
        let tag = this.tags.find(tag => tag.id === id && tag.creator === user.uuid && !tag.deleted)
        if (!tag || tag.creator !== user.uuid) throw new Error(`tag id ${id} not found`)
      })      
    } catch (err) {
      err.status = 400
      return process.nextTick(() => callback(err))
    }

    let tags = Array.from(new Set(props.tags)).sort()

    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)

      let filePath = path.join(this.absolutePath(dir), props.name)
      readXstat(filePath, (err, xstat) => {
        if (err) return callback(err)  
        if (xstat.type !== 'file') {
          let err = new Error('not a file')
          err.code = 'ENOTFILE'
          return callback(err)
        }

        // // user tag ids
        // let userTags = this.tags
        //   .filter(tag => tag.creator === user.uuid)
        //   .map(tag => tag.uuid)

        //   console.log(userTags)

        // // remove all user tags out of old tags
        // let oldTags = xstat.tags
        //   ? xstat.tags.reduce((acc, id) => userTags.includes(id) ? acc : [...acc, id])
        //   : []

        // let newTags = Array.from(new Set([...oldTags, tags]))
        updateFileTags(filePath, xstat.uuid, tags, callback)
      })
    })
  }

  DIRSTATS (user, props, callback) {
    this.DIR(user, props, (err, dir) => err ? callback(err) : callback(null, dir.stats()))
  }

  DIRENTRY_GET (user, props, callback) {
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err)
      let { name, hash } = props
      // backup drive files use hash as filename
      let filename = this.isBackupDrive(props.driveUUID) ? hash : name
      if (!filename) return callback(Object.assign(new Error('filename not found'), { status: 404 }))
      let filePath = path.join(this.absolutePath(dir), filename)
      fs.lstat(filePath, (err, stat) => {
        if (err && (err.code === 'ENOENT' || err.code === 'ENOTDIR')) err.status = 404
        callback(err, filePath)
      })
    })
  }

  /** end of new api for upload module **/

  //////////////////////////////////////////////////////////////////////////////
  //                                                                          // 
  // the following code are experimental
  //                                                                          //
  //////////////////////////////////////////////////////////////////////////////
  genTmpPath () {
    return path.join(this.tmpDir, UUID.v4())
  }

  /** retrieve absolute path **/
  absolutePath (node) {
    return node.abspath()
  }

  /**
  
  @param {object} user
  @param {object} props
  @param {string} driveUUID
  @param {string} dirUUID  
  @param {string[]} names
  @param {Policy} policy
  */
  MKDIRS (user, props, callback) {

    this.DIR(user, props.src, (err, srcDir) => {
      if (err) return callback(err)
      let srcUUIDs = srcDir.children.filter(c => props.names.includes(c.name)).map(d => d.uuid)
      this.DIR(user, props, (err, dir) => {
        if (err) return callback(err)
        if (!srcUUIDs.every(s => !dir.nodepath().map(d => d.uuid).includes(s))) return callback(new Error('can not move dir to subdir'))
        let { names, policy } = props
        let count = names.length
        let map = new Map()
        names.forEach(name => {
          let target = path.join(this.absolutePath(dir), name)
          mkdir(target, policy, (err, stat, resolved) => {
            map.set(name, { err, stat, resolved }) 
            if (!--count) {
              // TODO
              dir.read((err, xstats) => {
                if (err) return callback(err)
                callback(null, map)
              })
            }
          })
        })
      })
    })
  }

  /**
  @param {object} user
  @param {object} props
  @param {object} props.src
  @param {string} props.src.drive - src drive
  @param {string} props.src.dir - src (parent) dir
  @param {string} props.src.uuid - src file uuid
  @param {string} props.src.name - src file name
  @param {object} props.dst
  @param {string} props.dst.drive - dst drive
  @param {string} props.dst.dir - dst (parent) dir
  @param {Policy} props.policy
  */
  CPFILE (user, props, callback) {
    debug('CPFILE', props)
    let { src, dst, policy } = props
    this.DIR(user, { driveUUID: src.drive, dirUUID: src.dir }, (err, srcDir) => {
      if (err) return callback(err)
      this.DIR(user, { driveUUID: dst.drive, dirUUID: dst.dir }, (err, dstDir) => {
        if (err) return callback(err)

        let srcFilePath = path.join(this.absolutePath(srcDir), src.name)
        let dstFilePath = path.join(this.absolutePath(dstDir), src.name)

        let tmp = this.genTmpPath()
        clone(srcFilePath, src.uuid, tmp, (err, xstat) => {
          if (err) return callback(err)
          mkfile(dstFilePath, tmp, xstat.hash, policy, (err, xstat, resolved) => {
            rimraf(tmp, () => {})
            if (err) return callback(err)
            if (!xstat || (policy[0] === 'skip' && xstat && resolved[0])) {
              callback(null)
            } else {
/**
  why this code here ???
              try {
                let attr = JSON.parse(xattr.getSync(srcFilePath, 'user.fruitmix'))
                attr.uuid = xstat.uuid
                xattr.setSync(dstFilePath, 'user.fruitmix', JSON.stringify(attr))
              } catch (e) {
                if (e.code !== 'ENODATA') return callback(e)
              }
*/
              callback(null, xstat, resolved)
            }
          })
        })
           
      })
    })
  }

  /**
  @param {object} user
  @param {object} props
  @param {object} props.src
  @param {string} props.src.drive
  @param {string} props.src.dir
  @param {object} props.dst
  @param {string} props.dst.drive
  @param {string} props.dst.dir
  @param {Policy} props.policy
  */ 
  MVDIRS (user, props, callback) {
    let { src, dst, names, policy } = props
    this.DIR(user, { driveUUID: src.drive, dirUUID: src.dir }, (err, srcDir) => {
      if (err) return callback(err)
      let srcUUIDs = srcDir.children.filter(c => props.names.includes(c.name)).map(d => d.uuid)
      this.DIR(user, { driveUUID: dst.drive, dirUUID: dst.dir }, (err, dstDir) => {
        if (err) return callback(err)
        if (!srcUUIDs.every(s => !dstDir.nodepath().map(d => d.uuid).includes(s))) return callback(new Error('can not move dir to subdir'))
        let count = names.length 
        let map = new Map()
        names.forEach(name => {
          let oldPath = path.join(this.absolutePath(srcDir), name)
          let newPath = path.join(this.absolutePath(dstDir), name)
          mvdir(oldPath, newPath, policy, (err, stat, resolved) => {
            map.set(name, { err, stat, resolved })
            if (!--count) srcDir.read(() => dstDir.read(() => callback(null, map)))
          })
        })
      })
    })
  }

  /**
  @param {object} user
  @param {object} props
  @param {object} props.src
  @param {object} 
  */
  MVFILE (user, props, callback) {
    let { src, dst, policy } = props
    this.DIR(user, { driveUUID: src.drive, dirUUID: src.dir }, (err, srcDir) => {
      if (err) return callback(err)
      this.DIR(user, { driveUUID: dst.drive, dirUUID: dst.dir }, (err, dstDir) => {
        if (err) return callback(err)
        let oldPath = path.join(this.absolutePath(srcDir), src.name)
        let newPath = path.join(this.absolutePath(dstDir), src.name)
        // TODO to do what ???
        mvfile(oldPath, newPath, policy, (...args) => {
          srcDir.read(err => {
            dstDir.read(err => {
              callback(...args)
            })
          })
        })
      })
    })
  }

  /**
  @param {object} user
  @param {object} props
  @param {string} props.driveUUID
  @param {string} props.dirUUID
  @param {string} props.uuid
  @param {string} props.name
  */
  CLONE (user, props, callback) {
    this.DIR(user, props, (err, target) => {
      if (err) return callback(err)

      let dstPath = this.TMPFILE()
      let srcPath = path.join(this.absolutePath(target), props.name)
      clone(srcPath, props.uuid, dstPath, err => {
        if (err) return callback(err)
        callback(null, dstPath)
      })
    })
  }

  /**
  This function calls rmdir to remove an directory, the directory won't be removed if non-empty

  @param {object} user
  @param {object} props
  @param {string} props.driveUUID
  @param {string} props.dirUUID
  @param {string} props.name - for debug/display only
  */
  RMDIR (user, props, callback) {
    this.DIR(user, props, (err, dir) => {
      if (err) return callback(err) 
      if (!dir.parent) {
        let err = new Error('root dir cannot be removed')
        callback(err) 
      } else {
        let pdir = dir.parent
        let dirPath = this.absolutePath(dir)
        fs.rmdir(dirPath, err => {
          callback(null)
          if (!err) pdir.read() // FIXME pdir may be off tree or destroyed
        }) 
      }
    }) 
  }

  /**
  Query process arguments and pass request to iterate or visit accordingly.

  if ordered by time, start, count, end, places, types, tags, namepath
  if ordered by struct, last, count, places, types, tags, namepath, fileOnly, dirOnly

  @param {object} user
  @param {object} props
  @param {string} props.order - newest or oldest, default newest (not used now)
  @param {string} props.starti - inclusive start
  @param {string} props.starte - exclusive start
  @param {string} props.last -
  @param {string} props.count - number
  @param {string} props.endi - inclusive end
  @parma {string} props.ende - exclusive end
  @param {string} props.places - concatenated uuids separated by dot
  @param {string} props.types - concatenated types separated by dot
  @param {string} props.tags - concatenated numbers separated by dot
  @param {boolean} props.fileOnly
  @param {boolean} props.dirOnly
  */
  QUERY (user, props, callback) {
    debug('QUERY', props)
    const UUID_MIN = '00000000-0000-4000-0000-000000000000'
    const UUID_MAX = 'ffffffff-ffff-4fff-ffff-ffffffffffff'
  
    let order
    let startTime, startUUID, startExclusive
    let lastIndex, lastType, lastPath, fileOnly
    let count, places, types, tags, name, namepath, countOnly, groupBy

    const EInval = message => process.nextTick(() => 
      callback(Object.assign(new Error(message), { status: 400 })))

    if (!props.places) return EInval('places not provided')
    places = props.places.split('.')
    if (!places.every(place => isUUID(place))) return EInval('invalid places') 
    if (places.length !== Array.from(new Set(places)).length) return EInval('places has duplicate elements')
    // check permission
    for (let i = 0; i < places; i++) {
      let place = places[i]

      let dir = this.forest.uuidMap.get(place)
      if (!dir) return EInval(`place ${place} not found`)

      let drive = this.drives.find(d => d.uuid === dir.root().uuid)
      if (!this.userCanWriteDrive(user, drive)) return EInval(`place ${place} not found`)
    }

    if (props.order) {
      if (['newest', 'oldest', 'find'].includes(props.order)) {
        order = props.order
      } else {
        return EInval('invalid order')
      }
    } else {
      order = 'newest'
    }

    if (order === 'newest' || order === 'oldest') { // ordered by time
      if (props.starti) {
        let split = props.starti.split('.')
        if (split.length > 2) return EInval('invalid starti')

        startTime = parseInt(split[0])
        if (!Number.isInteger(startTime)) return EInval('invalid starti')

        if (split.length > 1) {
          startUUID = split[1]
          if (!isUUID(startUUID)) return EInval('invalid starti')
        } else {
          startUUID = order === 'newest' ? UUID_MAX : UUID_MIN
        }
        startExclusive = false
      } else if (props.starte) {
        let split = props.starte.split('.') 
        if (split.length > 2) return EInval('invalid starte')

        startTime = parseInt(split[0])
        if (!Number.isInteger(startTime)) return EInval('invalid starte')

        if (split.length > 1) {
          startUUID = split[1]
          if (!isUUID(startUUID)) return EInval('invalid starte')
        } else {
          startUUID = order === 'newest' ? UUID_MAX : UUID_MIN
        }
        startExclusive = true
      }
    } else { // ordered by fs structure
      if (props.last) {
        let str = props.last

        let dotIndex = str.indexOf('.')
        if (dotIndex === -1) return EInval('invalid last')

        lastIndex = parseInt(props.last.slice(0, dotIndex))
        if (!Number.isInteger(lastIndex) || lastIndex < 0) return EInval('invalid last')

        str = str.slice(dotIndex + 1)
        dotIndex = str.indexOf('.')
        if (dotIndex === -1) return EInval('invalid last')
        lastType = str.slice(0, dotIndex)
        if (lastType !== 'directory' && lastType !== 'file') return EInval('invalid last type')

        lastPath = str.slice(dotIndex + 1)
        if (path.isAbsolute(lastPath) || path.normalize(lastPath) !== lastPath) 
          return EInval('invalid last path')

        lastPath = lastPath.split('/')
      }
    }

    if (props.count) {
      count = parseInt(props.count)
      if (!Number.isInteger(count) || count <= 0) return EInval('invalid count')
    }

    if (props.countOnly) {
      countOnly = props.countOnly === 'true'
    }

    if (props.groupBy) {
      if (props.groupBy !== 'place') return EInval('invalid groupBy')
      groupBy = 'place'
    }

    if (props.class) {
      if (!['image', 'video', 'audio', 'document'].includes(props.class)) return EInval('invalid class')

      if (props.class === 'image') {
        types = ['JPEG', 'PNG', 'GIF', 'TIFF', 'BMP', 'HEIC']
      } else if (props.class === 'video') {
        types = ['RM', 'RMVB', 'WMV', 'AVI', 'MPEG', 'MP4', '3GP', 'MOV', 'FLV', 'MKV']
      } else if (props.class === 'audio') {
        types = ['RA', 'WMA', 'MP3', 'MKA', 'WAV', 'APE', 'FLAC']
      } else if (props.class === 'document') {
        types = ['DOC', 'DOCX', 'XLS', 'XLSX', 'PPT', 'PPTX', 'PDF', 'KEY', 'NUMBERS', 'PAGES']
      }
    } else if (props.types) {
      types = props.types.split('.')
      if (!types.every(type => !!type.length)) return EInval('invalid types')
    } 

    if (props.tags) {
      tags = props.tags.split('.').map(ts => parseInt(ts))
      if (tags.length !== Array.from(new Set(tags)).length) return EInval('invalid tags')
    }

    if (props.name) name = props.name

    if (order === 'newest' || order === 'oldest') {
      this.iterateList(user, { order, startTime, startUUID, startExclusive, 
        count, places, types, tags, name, namepath, countOnly, groupBy }, callback)
    } else {
      fileOnly = props.fileOnly === 'true'

      let args = { order, lastIndex, lastType, lastPath, count, places, types, tags, name }

      let range = { lastIndex, lastType, lastPath, count }
      let condition = { places, types, tags, name, fileOnly, countOnly, groupBy }

      setImmediate(() => {
        let arr
        try {
          arr = this.iterateTreeSync(user, range, condition)
          callback(null, arr)
        }catch (e) {
          callback(e)
        }
      })
    }
  }

  /**
  
  */
  iterateList (user, props, callback) {
    debug('iterate', props)

    let { order, startTime, startUUID, startExclusive } = props
    let { count, places, types, tags, name, countOnly, groupBy } = props
    let files = this.forest.timedFiles
    let startIndex, results

    let tmpF = this.TMPFILE()
    let resultFd, writeComma = false
    // matched file count
    let findCounter = 0
    // add countOnly && groupBy
    if (countOnly) {
      if (groupBy === 'place') {
        results = []
      } else{
        results = 0
      }
    } else {
      // create tmpfile , write result in this file
      // return this tmpfile
      resultFd = fs.createWriteStream(tmpF)
      resultFd.write('[')
    }

    const match = file => {
      if (name && !file.name.toLowerCase().includes(name.toLowerCase())) return

      if (types) {
        if (!file.metadata) return
        if (!types.includes(file.metadata.type)) return
      }

      if (tags) {
        if (!file.tags) return
        if (!tags.every(tag => file.tags.includes(tag))) return
      }

      // search and rename and search again will crash here FIXME
      let uuids = file.nodepath().map(n => n.uuid).slice(0, -1)
      let index = places.findIndex(place => uuids.includes(place))
      if (index === -1) return

      findCounter++
      // client need count only
      if (countOnly) {
        if (groupBy === 'place') {
          let key = places[index]
          let result = results.find( x => x.key === key)
          if (!result) {
            return results.push({ key, count: 1 })
          } else
            return result.count ++
        } else
          return results ++
      }

      let namepath = file.nodepath().map(n => n.name).slice(uuids.indexOf(places[index]) + 1)
      let xstat = {
        uuid: file.uuid,
        pdir: file.parent.uuid,
        name: file.name, 
        size: file.size,
        mtime: file.mtime,
        hash: file.hash,
        tags: file.tags,
        metadata: file.metadata,
        place: index,
        archived: file.archived,
        bctime: file.bctime,
        bmtime: file.bmtime,
        namepath 
      }
      if (writeComma) resultFd.write(',')
      else writeComma = true // next need write comma first
      resultFd.write(JSON.stringify(xstat))
      // arr.push(xstat)
    }
    
    if (order === 'newest') { // reversed order
      if (startTime === undefined) {
        startIndex = files.length - 1
      } else {
        startIndex = files.indexOfByKey(startTime, startUUID)
        if (startIndex === files.length) {
          startIndex--
        } else if (startExclusive) {
          let file = files.array[startIndex]
          if (file.getTime() === startTime && file.uuid === startUUID) startIndex--
        }
      }
      for (let i = startIndex; i >= 0; i--) {
        match(files.array[i])
        if (count && findCounter >= count) break
      }
    } else {
      if (startTime === undefined) {
        startIndex = 0
      } else {
        startIndex = files.indexOfByKey(startTime, startUUID)
        if (startExclusive && startIndex < files.length) {
          let file = files.array[startIndex]
          if (file.getTime() === startTime && file.uuid === startUUID) startIndex++
        }
      }
      for (let i = startIndex; i < files.length; i++) {
        match(files.array[i])
        if (count && findCounter >= count) break
      }
    }

    if (countOnly)
      process.nextTick(() => callback(null, results))
    else
      resultFd.write(']', err => {
        resultFd.close()
        if (err) return callback(err)
        callback(null, tmpF)
      })
  }

  /**
  This function has a weird concurrency. One async function parallels with a bunch of callbacks

  */
  iterateTreeSync (user, range, condition) {

    let { lastIndex, lastType, lastPath, count } = range
    let { places, types, tags, name, fileOnly, countOnly, groupBy } = condition
    
    let roots = places.map(place => this.forest.uuidMap.get(place)) 
    let arr = []
    let root, rootIndex

    const F = (node, dir) => {
      let xstat
      if (node instanceof Directory) {
        if (types || tags || fileOnly) return 
        if (name && !node.name.toLowerCase().includes(name.toLowerCase())) return
        xstat = { 
          uuid: node.uuid, 
          pdir: node.parent.uuid,
          type: 'directory', 
          name: node.name, 
          mtime: Math.abs(node.mtime),
          archived: node.archived,
          bctime: node.bctime,
          bmtime: node.bmtime,
        }
      } else if (node instanceof File) {
        if (tags) {
          if (!node.tags) return
          if (!tags.every(tag => node.tags.include(tag))) return
        }
        if (types) {
          if (!node.metadata) return 
          if (!types.includes(node.metadata.type)) return
        }
        if (name && !node.name.toLowerCase().includes(name.toLowerCase())) return
        xstat = { 
          uuid: node.uuid,
          pdir: node.parent.uuid,
          type: 'file',
          name: node.name,
          size: node.size,
          mtime: node.mtime,
          hash: node.hash,
          tags: node.tags,
          metadata: node.metadata,
          archived: node.archived,
          bctime: node.bctime,
          bmtime: node.bmtime,
        } 
      }
      /* unindexed-file name
      else { // string - unindexed file
        if (tags || types) return
        if (name && !node.toLowerCase().includes(name.toLowerCase())) return
        xstat = {
          pdir: dir,
          type: 'file',
          name: node,
        }
      }*/

      if (xstat) {
        let nodepath, namepath

        if (typeof node === 'object') {
          nodepath = node.nodepath()
          namepath = nodepath.slice(nodepath.indexOf(root) + 1).map(n => n.name)
        } else {
          nodepath = dir.nodepath() 
          namepath = nodepath.slice(nodepath.indexOf(root) + 1).map(n => n.name)
          namepath.push(node)

          // for backup dirs /translate unindexed filename -> hash(bashup drive use hash name)
          // FIXME: backup unindexfilename -> file hash
          if (this.backup.isBackupDir(dir)) {

          }
        }

        xstat.place = rootIndex
        xstat.namepath = namepath

        arr.push(xstat)
        if (arr.length === count) return true
      }
    }

    if (lastIndex === undefined) {
      for (rootIndex = 0; rootIndex < roots.length; rootIndex++) {
        root = roots[rootIndex]
        if (root.iterate({ namepath: [], type: 'directory' }, F)) break
      }
    } else {
      rootIndex = lastIndex
      root = roots[rootIndex]
      if (!(root.iterate({ namepath: lastPath, type: lastType }, F))) {
        for (rootIndex = lastIndex + 1; rootIndex < roots.length; rootIndex++) {
          root = roots[rootIndex]
          if (root.iterate({ namepath: [], type: 'directory' }, F)) break
        }
      }
    }

    return arr
  }

  /**
  @param {object} props
  @param {string} props.fingerprint - media hash / fingerprint
  @param {boolean} props.file - if true, return file path
  @param {boolean} props.both - if true, return both meta and file path
  */
  getMedia (user, props, callback) {
    debug('get media', props)

    let err, data
    let { fingerprint, file, both } = props

    if (!isSHA256(fingerprint)) {
      err = Object.assign(new Error('invalid hash'), { status: 400 })
    } else if (!this.metaMap.has(fingerprint)) {
      err = Object.assign(new Error('media not found'), { status: 404 })
    } else {
      // drive uuids

      if (user) {
        let uuids = this.drives.filter(drv => this.userCanWriteDrive(user, drv)).map(drv => drv.uuid)
        let meta = this.metaMap.get(fingerprint)

        if (meta.files.some(f => uuids.includes(f.root().uuid))) {
          let metadata = Object.assign({}, meta.metadata, { size: meta.files[0].size })
          let filePath = this.absolutePath(meta.files[0])

          if (file) {
            data = filePath
          } else if (both) {
            data = { metadata, path: filePath }
          } else {
            data = metadata
          }
        } else {
          err = Object.assign(new Error('permission denied'), { status: 403 })
        }
      } else {
        data = this.absolutePath(this.metaMap.get(fingerprint).files[0])
      }
    }

    process.nextTick(() => callback(err, data))
  }

  dirFormat (user, props, callback) {
    if (props.driveUUID !== props.dirUUID) {
      return callback(Object.assign(new Error('invalid dirUUID'), { status: 400 }))
    }

    let dir, root, drive

    // find dir
    dir = this.forest.uuidMap.get(props.dirUUID)
    if (!dir) {
      let err = new Error('dir not found')
      err.status = 404
      return process.nextTick(() => callback(err))
    }

    // find root
    root = dir.root()
   
    // find drive 
    drive = this.drives.find(d => d.uuid === root.uuid)

    /**
    If driveUUID is provided, the corresponding drive must contains dir.
    */
    if (props.driveUUID && props.driveUUID !== drive.uuid) {
      let err = new Error('drive does not contain dir')
      err.status = 403
      return process.nextTick(() => callback(err))
    }

    if (!this.userCanWriteDrive(user, drive)) {
      let err = new Error('permission denied') 
      err.status = 403      // TODO 404?
      return process.nextTick(() => callback(err))
    }

    if (drive.privacy !== true) {
      let err = new Error('permission denied') 
      err.status = 403      // TODO 404?
      return process.nextTick(() => callback(err))
    }

    if (props.op && props.op === 'format') {
      let tmpDir = path.join(this.tmpDir, UUID.v4())
      let dirPath = path.join(this.driveDir, root.uuid)
      try {
        mkdirp.sync(tmpDir)
        let attr = JSON.parse(xattr.getSync(dirPath, 'user.fruitmix'))
        xattr.setSync(tmpDir, 'user.fruitmix', JSON.stringify(attr))
        rimraf.sync(dirPath)
        fs.renameSync(tmpDir, dirPath)
        return dir.read(callback)
      } catch (e) {
        return callback(e)
      }
    }
    return callback(new Error('invalid op'))
  }
}

module.exports = VFS
