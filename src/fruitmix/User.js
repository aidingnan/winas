const EventEmitter = require('events')
const UUID = require('uuid')
const { isUUID, isNonNullObject, isNonEmptyString } = require('../lib/assertion')
const DataStore = require('../lib/DataStore')
const { passwordEncrypt, md4Encrypt } = require('../lib/utils') // eslint-disable-line
const request = require('superagent')
const debug = require('debug')('appifi:user')
const assert = require('assert')
const Config = require('config')

const USER_STATUS = {
  ACTIVE: 'ACTIVE',
  INACTIVE: 'INACTIVE',
  DELETED: 'DELETED'
}

// while cloud users update , change to reading state
class Base {
  constructor (user, ...args) {
    this.user = user
    user.state = this
    this.enter(...args)
  }

  enter () {
  }

  exit () {
  }

  setState (nextState, ...args) {
    this.exit()
    let NextState = this.user[nextState]
    new NextState(this.user, ...args)
  }

  readi () {
    this.setState('Reading')
  }

  readn (delay) {
    this.setState('Pending', delay)
  }

  readc (callback) {
    this.setState('Reading', [callback])
  }

  destroy () {
    this.exit()
  }
}

class Idle extends Base {
  enter () {
    this.timer = setTimeout(() => {
      this.readi()
    }, 1000 * 60 * 60 * 3)
  }

  exit () {
    if (this.timer) clearTimeout(this.timer)
  }
}

class Pending extends Base {
  enter (delay) {
    assert(Number.isInteger(delay) && delay > 0)
    this.readn(delay)
  }

  exit () {
    clearTimeout(this.timer)
  }

  readn (delay) {
    assert(Number.isInteger(delay) && delay > 0)
    clearTimeout(this.timer)
    this.timer = setTimeout(() => this.readi(), delay)
  }
}

class Reading extends Base {
  enter (callbacks = []) {
    this.callbacks = callbacks
    this.pending = undefined
    this.request = null
    this.fetch()
  }

  fetch() {
    this.request = request
      .get(`${Config.pipe.baseURL}/s/v1/station/user`)
      .set('Authorization', (this.user.cloudConf && this.user.cloudConf.cloudToken) || '')
      .end((err, res) => {
        if (err || !res.ok) {
          err = err || new Error('cloud return error')
          err.status = 503
          this.readn(1000 * 60)
        } else {
          let data = res.body.data
          if (data) {
            this.updateUsers(data)
          }
        }

        this.callbacks.forEach(callback => callback(err, res.body.data))
        if (Array.isArray(this.pending)) { // stay in working
          this.enter(this.pending)
        } else {
          if (typeof this.pending === 'number') {
            this.setState('Pending', this.pending)
          } else {
            this.setState('Idle')
          }
        }
      })
  }

  updateUsers(data) {
    let owner = data.owner.find(x => !x.delete)
    if (owner) {
      owner.isFirstUser = true
      // check owner
      let firstUser = this.user.users.find(u => u.isFirstUser)
      if (firstUser.winasUserId !== owner.id) {
        throw new Error('device owner change!!!!')
      }
      let users = [owner, ...data.sharer]
      this.user.storeSave(lusers => {
        // update or create
        users.forEach(u => {
          if (!u.username) throw new Error('user phoneNumber can not be null')
          let x = lusers.find(lx => lx.winasUserId === u.id)
          if (x) {
            x.avatarUrl = u.avatarUrl
            x.username = u.nickName || x.username
            if (!x.isFirstUser) {
              x.cloud = !!u.cloud
              x.publicSpace = !!u.publicSpace
            }
            // phone update
            if(x.phoneNumber !== u.username) {
              x.phoneNumber = u.username
            }

            x.status = u.delete === 1 ? USER_STATUS.DELETED
              : u.disable === 1? USER_STATUS.INACTIVE
                : USER_STATUS.ACTIVE
          } else {
            // PhoneNumber Exist Error
            let pu = lusers.find(x => x.phoneNumber === u.username)
            if (pu && pu.status !== USER_STATUS.DELETED) throw new Error('phoneNumber already exist')

            let newUser = {
              uuid: UUID.v4(),
              username: u.nickName || u.username,
              isFirstUser: false,
              winasUserId: u.id,
              avatarUrl: u.avatarUrl,
              phoneNumber: u.username,
              winasUserId: u.id,
              cloud: !!u.cloud,
              publicSpace: !!u.publicSpace
            } 
            //set user state
            newUser.status = u.delete === 1 ? USER_STATUS.DELETED
              : u.disable === 1? USER_STATUS.INACTIVE
                : USER_STATUS.ACTIVE
            lusers.push(newUser)
          }
        })
        return [...lusers]
      }, err => err ? console.log(err) : '')
    }
    else {
      if (this.user.users.length) {
        // do what?
        throw new Error('could not found owner in cloud')
      }
      console.log('no user bound')
    }
  }

  exit () {
  }

  readi () {
    if (!Array.isArray(this.pending)) this.pending = []
  }

  readn (delay) {
    if (Array.isArray(this.pending)) {

    } else if (typeof this.pending === 'number') {
      this.pending = Math.min(this.pending, delay)
    } else {
      this.pending = delay
    }
  }

  readc (callback) {
    if (Array.isArray(this.pending)) {
      this.pending.push(callback)
    } else {
      this.pending = [callback]
    }
  }

  destroy () {
    let err = new Error('destroyed')
    err.code = 'EDESTROYED'
    this.callbacks.forEach(cb => cb(err))
    if (Array.isArray(this.pending)) this.pending.forEach(cb => cb(err))
    this.request.abort()
    super.destroy()
  }
}

/**

The corresponding test file is test/unit/fruitmix/user.js

Using composition instead of inheritance.
*/
class User extends EventEmitter {
  /**
  Create a User

  Add other properties to opts if required.

  @param {object} opts
  @param {string} opts.file - path of users.json
  @param {string} opts.tmpDir - path of tmpDir (should be suffixed by `users`)
  @param {boolean} opts.isArray - should be true since users.json is an array
  */
  constructor (opts) {
    super()
    this.conf = opts.configuration
    this.cloudConf = opts.cloudConf
    this.fruitmixDir = opts.fruitmixDir
    this.store = new DataStore({
      file: opts.file,
      tmpDir: opts.tmpDir,
      isArray: true
    })

    this.store.on('Update', (...args) => this.emit('Update', ...args))

    this.once('Update', () => new Pending(this, 200))

    Object.defineProperty(this, 'users', {
      get () {
        return this.store.data || []
      }
    })
  }

  usersUpdate() {
    this.state && this.state.readi()
  }

  getUser (userUUID) {
    return this.users.find(u => u.uuid === userUUID && u.status !== USER_STATUS.DELETED)
  }

  /**
   * data 为数组或者方法
   * 所有的存储任务提交前先检查约束条件是否都过关
   */
  storeSave (data, callback) {
    this.store.save(users => {
      let changeData = typeof data === 'function' ? data(users) : data
      // check rules
      if (changeData) {
        if (changeData.filter(u => u.status === USER_STATUS.ACTIVE).length > 10) {
          throw Object.assign(new Error('active users max 10'), { status: 400 })
        }
      }
      return changeData
    }, callback)
  }

  /**
  TODO lastChangeTime is required by smb
  TODO createTime is required by spec
  */
  createUser (props, callback) {
    let uuid = UUID.v4()
    this.storeSave(users => {
      let isFirstUser = users.length === 0
      let { username, winasUserId, phoneNumber } = props // eslint-disable-line

      let cU = users.find(u => u.username === username)
      if (cU && cU.status !== USER_STATUS.DELETED) throw new Error('username already exist')
      let pnU = users.find(u => u.phoneNumber === phoneNumber)
      if (pnU && pnU.status !== USER_STATUS.DELETED) throw new Error('phoneNumber already exist')
      let pU = users.find(u => u.winasUserId === winasUserId)
      if (pU && pU.status !== USER_STATUS.DELETED) throw new Error('winasUserId already exist')

      let newUser = {
        uuid,
        username: props.username,
        isFirstUser,
        status: USER_STATUS.ACTIVE,
        createTime: new Date().getTime(),
        lastChangeTime: new Date().getTime(),
        phoneNumber: props.phoneNumber,
        winasUserId: props.winasUserId // for winas
      }

      return [...users, newUser]
    }, (err, data) => {
      if (err) return callback(err)
      return callback(null, data.find(x => x.uuid === uuid))
    })
  }

  updateUser (userUUID, props, callback) {
    let { username, status, phoneNumber } = props
    this.storeSave(users => {
      let index = users.findIndex(u => u.uuid === userUUID)
      if (index === -1) throw new Error('user not found')
      let nextUser = Object.assign({}, users[index])
      if (nextUser.status === USER_STATUS.DELETED) throw new Error('deleted user can not update')
      if (username) {
        if (users.find(u => u.username === username && u.status !== USER_STATUS.DELETED)) throw new Error('username already exist')
        nextUser.username = username
      }
      if (phoneNumber) {
        if (users.find(u => u.phoneNumber === phoneNumber && u.status !== USER_STATUS.DELETED)) throw new Error('phoneNumber already exist')
        nextUser.phoneNumber = phoneNumber
      }
      if (status) nextUser.status = status
      return [...users.slice(0, index), nextUser, ...users.slice(index + 1)]
    }, (err, data) => {
      if (err) return callback(err)
      return callback(null, data.find(x => x.uuid === userUUID))
    })
  }

  updatePassword (userUUID, props, callback) {
      return callback(Object.assign(new Error('not found'), { status: 404 }))
  }

  bindFirstUser (boundUser) {
    this.storeSave(users => {
      let winasUserId = boundUser.id
      //FIXME: delete all firstuser flag
      users = users.map(u => (u.isFirstUser = false, u))

      let index = users.findIndex(u => u.winasUserId === winasUserId)
      if (index === -1) {
        return [...users, {
          uuid: UUID.v4(),
          username: boundUser.username || 'admin',
          isFirstUser: true,
          status: USER_STATUS.ACTIVE,
          winasUserId: winasUserId,
          phoneNumber: boundUser.phone
        }]
      } else { // update user info 
        let admin = users[index]
        if (boundUser.username) admin.username = boundUser.username
        if (boundUser.phone) admin.phoneNumber = boundUser.phone
        admin.status = USER_STATUS.ACTIVE
        admin.isFirstUser = true
        return [
          ...users.slice(0, index),
          admin,
          ...users.slice(index + 1)
        ]
      }
    },
    err => err
      ? console.log(`user module failed to bind first user to ${boundUser.id}`, err)
      : console.log(`user module bound first user to ${boundUser.id} successfully`))
  }

  destroy (callback) {
    this.store.destroy(callback)
    this.state && this.state.destroy()
  }

  basicInfo (user) {
    return {
      uuid: user.uuid,
      username: user.username,
      isFirstUser: user.isFirstUser,
      phoneNumber: user.phoneNumber,
      winasUserId: user.winasUserId,
      avatarUrl: user.avatarUrl
    }
  }

  fullInfo (user) {
    return {
      uuid: user.uuid,
      username: user.username,
      isFirstUser: user.isFirstUser,
      createTime: user.createTime,
      status: user.status,
      phoneNumber: user.phoneNumber,
      winasUserId: user.winasUserId,
      avatarUrl: user.avatarUrl
    }
  }

  /**
  Implement LIST method
  */
  LIST (user, props, callback) {
    if (!user) {
      // basic info of all users
      return process.nextTick(() => callback(null, this.users.filter(u => u.status === USER_STATUS.ACTIVE).map(u => this.fullInfo(u))))
    } else if (user.isFirstUser) {
      // full info of all users
      return process.nextTick(() => callback(null, this.users.filter(u => u.status !== USER_STATUS.DELETED).map(u => this.fullInfo(u))))
    } else {
      // full info of the user
      return process.nextTick(() => {
        let u = this.users.find(u => u.uuid === user.uuid)
        if (!u) {
          let err = new Error('authenticated user not found in user resource')
          err.status = 500
          callback(err)
        } else {
          callback(null, [this.fullInfo(u)])
        }
      })
    }
  }

  /**
  Implement POST method
  */
  POST (user, props, callback) {
    if (!isNonNullObject(props)) return callback(Object.assign(new Error('props must be non-null object'), { status: 400 }))
    let recognizedStatus = ['username', 'password', 'phoneNumber', 'winasUserId']
    // by design, can not update anything
    return callback(Object.assign(new Error('not found'), { status: 404 }))
    
    Object.getOwnPropertyNames(props).forEach(key => {
      if (!recognized.includes(key)) throw Object.assign(new Error(`unrecognized prop name ${key}`), { status: 400 })
    })
    if (!isNonEmptyString(props.username)) return callback(Object.assign(new Error('username must be non-empty string'), { status: 400 }))
    if (props.phicommUserId && !isNonEmptyString(props.phicommUserId)) return callback(Object.assign(new Error('phicommUserId must be non-empty string'), { status: 400 }))
    if (!isNonEmptyString(props.phoneNumber)) return callback(Object.assign(new Error('phoneNumber must be non-empty string'), { status: 400 }))
    if (props.password && !isNonEmptyString(props.password)) return callback(Object.assign(new Error('password must be non-empty string'), { status: 400 }))
    if (this.users.length && (!user || !user.isFirstUser)) return process.nextTick(() => callback(Object.assign(new Error('Permission Denied'), { status: 403 })))
    if (props.password) {
      props.password = passwordEncrypt(props.password, 10)
    }
    this.createUser(props, (err, user) => err ? callback(err) : callback(null, this.fullInfo(user)))
  }

  /**
  Implement GET method
  */
  GET (user, props, callback) {
    let userUUID = props.userUUID
    let u = isUUID(userUUID) ? this.getUser(props.userUUID)
      : this.users.find(u => u.phicommUserId && u.phicommUserId === props.userUUID && u.status !== USER_STATUS.DELETED)
    if (!u) return process.nextTick(() => callback(Object.assign(new Error('user not found'), { status: 404 })))
    if (user.isFirstUser || user.uuid === u.uuid) return process.nextTick(() => callback(null, this.fullInfo(u)))
    return process.nextTick(Object.assign(new Error('Permission Denied'), { status: 403 }))
  }

  /**
  Implement PATCH
  */
  PATCH (user, props, callback) {
    return callback(Object.assign(new Error('not found'), { status: 404 }))
  }

  DELETE (user, props, callback) {
    return callback(Object.assign(new Error('not found'), { status: 404 }))
  }
}

User.prototype.USER_STATUS = USER_STATUS
User.prototype.Idle= Idle
User.prototype.Pending = Pending
User.prototype.Reading = Reading

module.exports = User
