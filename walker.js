
  const Promise = require('bluebird')
  const fs = Promise.promisifyAll(require('fs'))
  const path = require('path')

  let workerData = {
    path: '/home',
    match: 'app',
    count: 100
  }
  
  async function visitTreeAysnc(dir, func, context = {}) {

    let entries
    try {
      entries = await fs.readdirAsync(dir, { withFileTypes:true })
    } catch(e) {
      return
    }
    let files = [], dirs = []
    
    // filter files and dirs
    for (let i =0; i < entries.length; i ++) {
      if (entries[i].isFile())
        files.push(entries[i])
      else if (entries[i].isDirectory())
        dirs.push(entries[i])
    }

    //sort
    dirs.length && dirs.sort((a, b) => a.name.localeCompare(b.name))
    files.length && files.sort((a, b) => a.name.localeCompare(b.name))
    
    if (context.condition) {
      let { type, name, namepath } = context.condition
      if (namepath.length === 0) {
        if (type === 'directory') {
          let index = dirs.findIndex(x => name.localeCompare(x.name) <= 0)
          if (index !== -1)  dirs = dirs.slice(index) // find dir, drop previous dirs
          else dirs = [] // dir not found, clean all dirs
          if (files.length === 0) return
        } else {
          dirs = [] // clear dirs
          let index = files.findIndex(x => name.localeCompare(x.name) <= 0)
          if (index !== -1)  files = files.slice(index + 1) // find dir, drop previous dirs
          else files = [] // dir not found, clean all dirs
        }
        delete context.condition
      } else {
        let index = dirs.findIndex(x => namepath[0].localeCompare(x.name) <= 0)
        if (index !== -1) {
          dirs = dirs.slice(index) // find dir, drop previous dirs
          context.condition.namepath = namepath.slice(1) // delete used namepath
        } else dirs = [] // dir not found
      }
    }

    if (!(dirs.length + files.length)) return 
    for (let i = 0; i < dirs.length; i++) {
      if (func(dirs[i]) && (!context.condition || i > 0)) context.match.push(dirs[i])
      await visitTreeAysnc(path.join(dir, dirs[i].name), func, context)
      if (context.condition) delete context.condition
    }

    for (let i = 0; i < files.length; i++) {
      if (func(files[i])) {
        files[i].pdirp = dir
        context.match.push(files[i])
      }
    }
  }


  let context = { 
    match:[]
  }


  let condition = {
    type: 'file',
    namepath: ['documentation', 'node_modules', 'protagonist', 'test', 'performance', 'fixtures'],
    name: 'fixture-2.apib'
  }

  // context.condition = condition

  visitTreeAysnc(workerData.path, entry => entry.name.toUpperCase().includes(workerData.match.toUpperCase()), context)
    .then(() => console.log(context.match.length, context.match.map(x =>[x.pdirp, x.name])))
