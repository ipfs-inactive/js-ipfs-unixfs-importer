'use strict'

const batch = require('it-flat-batch')

module.exports = async function * (source, reduce) {
  const roots = []

  for await (const chunk of batch(source, Number.MAX_SAFE_INTEGER)) {
    roots.push(await reduce(chunk))
  }

  yield roots[0]
}
