'use strict'

const pull = require('pull-stream/pull')
const map = require('pull-stream/throughs/map')
const toPull = require('async-iterator-to-pull-stream')
const toIterator = require('pull-stream-to-async-iterator')
const importer = require('./importer')

module.exports = function (source, ipld, options = {}) {
  return toIterator(
    pull(
      toPull.source(source),
      map(({ path, content }) => {
        if (content && content[Symbol.asyncIterator]) {
          content = toPull(content)
        }

        return {
          path,
          content
        }
      }),
      importer(ipld, options)
    )
  )
}
