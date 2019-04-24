/* eslint-env mocha */
'use strict'

const createChunker = require('../src/chunker/rabin')
const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const isNode = require('detect-node')
const toIterator = require('pull-stream-to-async-iterator')
const all = require('async-iterator-all')

const chunker = (source, options) => {
  return toIterator(
    pull(
      values(source),
      createChunker(options)
    )
  )
}

describe('chunker: rabin browser', () => {
  before(function () {
    if (isNode) {
      this.skip()
    }
  })

  it('returns an error', async () => {
    const b1 = Buffer.alloc(2 * 256)
    const b2 = Buffer.alloc(1 * 256)
    const b3 = Buffer.alloc(5 * 256)

    b1.fill('a')
    b2.fill('b')
    b3.fill('c')

    try {
      await all(chunker([b1, b2, b3], {
        minChunkSize: 48,
        avgChunkSize: 96,
        maxChunkSize: 192
      }))
    } catch (err) {
      expect(err.message).to.include('Rabin chunker not available')
    }
  })
})
