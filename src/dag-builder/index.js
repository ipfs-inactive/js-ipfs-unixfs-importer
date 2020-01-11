'use strict'

const dirBuilder = require('./dir')
const fileBuilder = require('./file')
const createChunker = require('../chunker')
const validateChunks = require('./validate-chunks')

async function * dagBuilder (source, ipld, options) {
  for await (const entry of source) {
    if (entry.path) {
      if (entry.path.substring(0, 2) === './') {
        options.wrapWithDirectory = true
      }

      entry.path = entry.path
        .split('/')
        .filter(path => path && path !== '.')
        .join('/')
    }

    if (entry.content) {
      let source = entry.content

      // wrap in iterator if it is array-like or not an iterator
      if ((!source[Symbol.asyncIterator] && !source[Symbol.iterator]) || source.length !== undefined) {
        source = {
          [Symbol.iterator]: function * () {
            yield entry.content
          }
        }
      }

      if (options.fromParts) {
        options.rawLeaves = true
        options.chunker = source => source
        options.reduceSingleLeafToSelf = false
      } else {
        source = validateChunks(source)
      }
      const chunker = createChunker(options.chunker, source, options)

      // item is a file
      yield () => fileBuilder(entry, chunker, ipld, options)
    } else {
      // item is a directory
      yield () => dirBuilder(entry, ipld, options)
    }
  }
}

module.exports = dagBuilder
