const Promise = require('bluebird')
const path = require('path')
const fs = require('fs')

const rimraf = require('rimraf')
const mkdirp = require('mkdirp')
const mkdirpAsync = Promise.promisify(require('mkdirp'))
const UUID = require('uuid')
const xattr = require('fs-xattr')

const chai = require('chai').use(require('chai-as-promised'))
const expect = chai.expect
const should = chai.should()

const MapPersistent = require('src/lib/MapPersistence')

const cwd = process.cwd()
const tmptest = path.join(cwd, 'tmptest')
const tmpDir = path.join(cwd, 'tmp')

const testdata = []
const testdata2 = []
for(let i = 0; i < 10 ; i++) {
  testdata.push({
    k: i + "f7831399d54042826cd18d473986d81473c8e92c774a6f5db61cb70863180e0",
    v: { ctime: 1557992607263, mtime: 1557992607263 }
  })
  testdata2.push({
    k: '1234' + i + UUID.v4(),
    v: { ctime: 1557992607263, mtime: 1557992607263 }
  })
}

describe('test MapPersistent', () => {
  let mapP
  beforeEach(() => {
    rimraf.sync(tmptest)
    rimraf.sync(tmpDir)
    mkdirp.sync(tmptest)
    mkdirp.sync(tmpDir)
    mapP = new MapPersistent(tmptest, tmpDir)
  })

  it(`set testdata to persistent map`, async function() {
    this.timeout(0)
    for (let i = 0; i < testdata.length; i++) {
      await mapP.setAsync(testdata[i])
    }
    for (let i = 0; i < testdata.length; i++) {
      await mapP.updateAsync(testdata[i])
    }
    for (let i = 0; i < testdata.length; i++) {
      await mapP.deleteAsync(testdata[i])
    }
    for (let i = 0; i < testdata.length; i++) {
      console.log(await mapP.getAsync(testdata[i]))
    }
  })

  it(`concurrence`, done => {
    let count= testdata2.length
    for (let i = 0; i < testdata2.length; i++) {
      mapP.set(testdata2[i], () => {
        if (--count === 0) {
          done()
        }
      })
    }
  })
})