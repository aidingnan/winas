const Promise = require('bluebird')
const path = require('path')
const fs = require('fs')

const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const mkdirpAsync = Promise.promisify(require('mkdirp'))
const UUID = require('uuid')

const chai = require('chai').use(require('chai-as-promised'))
const expect = chai.expect
const should = chai.should()
const AutoThumb = require('src/fruitmix/AutoThumb')

const query = {
  width: 200,
  height: 200,
  autoOrient: 'true',
  modifier: 'caret'
}

const cwd = process.cwd()
const tmptest = path.join(cwd, 'tmptest')
const thumbDir = path.join(tmptest, 'thumbnail')
const tmp = path.join(cwd, 'tmp')
const testdata = path.join(cwd, 'testdata')

const testfiles = [
  {
    "uuid": "210633d3-5033-4540-88ee-0a513ce878c0",
    "type": "file",
    "name": "1.jpeg",
    "mtime": 1566394248144,
    "size": 173187,
    "hash": "9734e197a910eef6ec73c138d68415a09da31ac46f3c9b4789f3ee5c10026b04",
    "metadata": {
      "type": "JPEG",
      "w": 1079,
      "h": 1443,
      "orient": 1
    }
  },
  {
    "uuid": "f4b2ec4c-4b3e-4cde-82a8-add3582929a5",
    "type": "file",
    "name": "2.jpeg",
    "mtime": 1566394248144,
    "size": 106981,
    "hash": "29b17ea4841ceb0cbe3a7d23aaf0e0121c977f037c417e6b8632528bf3d6a20e",
    "metadata": {
      "type": "JPEG",
      "w": 1080,
      "h": 1440
    }
  },
  {
    "uuid": "ed75efc6-ebb8-4331-a3ce-ad01ffcf1cda",
    "type": "file",
    "name": "3.jpeg",
    "mtime": 1566394248144,
    "size": 130384,
    "hash": "82664be7ab649b2e9a5247e6042788a64efede0a4d8c4c3884c15ebc6c18cb46",
    "metadata": {
      "type": "JPEG",
      "w": 1440,
      "h": 1080
    }
  },
  {
    "uuid": "0e8d235a-67a5-4db2-aad7-4fb899805b45",
    "type": "file",
    "name": "4.mp4",
    "mtime": 1566394248144,
    "size": 4250132,
    "hash": "39d7014e1e6c20bda933064c36fe4daa6692f2c249bfe9d831e59406c1077340",
    "metadata": {
      "type": "MP4",
      "w": 352,
      "h": 640,
      "date": "2019:08:21 12:31:32",
      "dur": 153,
      "rot": 0
    }
  }
]

const fackfile = {
  "uuid": "0e8d235a-67a5-4db2-aad7-4fb899805b45",
  "type": "file",
  "name": "1111.mp4",
  "mtime": 1566394248144,
  "size": 4250132,
  "hash": "39d7014e1e6c20bda933064c36fe4daa6692f2c249bfe9d831e59406c1077340",
  "metadata": {
    "type": "MP4",
    "w": 352,
    "h": 640,
    "date": "2019:08:21 12:31:32",
    "dur": 153,
    "rot": 0
  }
}

describe('test AutoThumb module', () => {
  let autoThumb
  beforeEach(() => {
    rimraf.sync(tmp)
    rimraf.sync(thumbDir)
    mkdirp.sync(tmp)
    mkdirp.sync(thumbDir)
    autoThumb = new AutoThumb(thumbDir, tmp, query)
  })

  it('create thumbs', function(done) {
    this.timeout(1000 * 30)
    testfiles.forEach(x => autoThumb.req(path.join(testdata, x.name), x.metadata, x.hash))
    let count = testfiles.length
    autoThumb.on('workFinished', (...args) => {
      if (--count === 0) {
        fs.readdir(thumbDir, console.log)
        done()
      }
    })
  })

  it('should pass not found file', function(done) {
    this.timeout(1000 * 30)
    autoThumb.req(path.join(testdata, fackfile.name), fackfile.metadata, fackfile.hash)
    autoThumb.on('workFinished', (sha256, err, path) => {
      expect(err.code).to.equal('ENOENT')
      done()
    })
  })

  it('should pass error line', function(done) {
    let tasksStream = autoThumb.tasksStream
    tasksStream.write('12345\n')
    tasksStream.write('1.auto.thumb.3.auto.thumb.4\n')
    tasksStream.write('\n')
    tasksStream.write('\n')
    autoThumb.req(path.join(testdata, fackfile.name), fackfile.metadata, fackfile.hash)
    testfiles.forEach(x => autoThumb.req(path.join(testdata, x.name), x.metadata, x.hash))
    let count = testfiles.length
    autoThumb.on('workFinished', (...args) => {
      if (--count === 0) {
        fs.readdir(thumbDir, console.log)
        done()
      }
    })
  })

  it('should renew tasksStream if it close', function(done) {
    this.timeout(10000000)
    let tasksStream = autoThumb.tasksStream
    tasksStream.write('12345\n')
    tasksStream.write('1.auto.thumb.3.auto.thumb.4\n')
    tasksStream.write('\n')
    tasksStream.write('\n')
    autoThumb.req(path.join(testdata, fackfile.name), fackfile.metadata, fackfile.hash)
    testfiles.forEach(x => autoThumb.req(path.join(testdata, x.name), x.metadata, x.hash))
    tasksStream.end()
    
    testfiles.forEach(x => autoThumb.req(path.join(testdata, x.name), x.metadata, x.hash))
    autoThumb.req(path.join(testdata, fackfile.name), fackfile.metadata, fackfile.hash)
    let count = testfiles.length + 2 
    autoThumb.on('workFinished', (...args) => {
      if (--count === 0) {
        fs.readdir(thumbDir, console.log)
        done()
      }
    })
  })

  afterEach(() => {
    autoThumb.destroy()
  })
})