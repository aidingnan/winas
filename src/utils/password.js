const { passwordEncrypt } = require('../lib/utils')

process.argv.forEach((arg, idx, arr) => {
  if (idx > 0 && __filename === arr[idx - 1] && typeof arg === 'string' && arg.length > 0)
    console.log(passwordEncrypt(arg, 10))
})
