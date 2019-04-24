/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const createBuilder = require('../src/builder')
const FixedSizeChunker = require('../src/chunker/fixed-size')
const toIterator = require('pull-stream-to-async-iterator')
const all = require('async-iterator-all')

const builder = (source, ipld, options) => {
  return toIterator(
    pull(
      values(source),
      createBuilder(FixedSizeChunker, ipld, options)
    )
  )
}

describe('builder: onlyHash', () => {
  let ipld

  before((done) => {
    inMemory(IPLD, (err, resolver) => {
      expect(err).to.not.exist()

      ipld = resolver

      done()
    })
  })

  it('will only chunk and hash if passed an "onlyHash" option', async () => {
    const nodes = await all(builder({
      path: '/foo.txt',
      content: Buffer.from([0, 1, 2, 3, 4])
    }, ipld, {
      onlyHash: true
    }))

    expect(nodes.length).to.equal(2)

    try {
      await ipld.get(nodes[0].cid)

      throw new Error('Should have errored')
    } catch (err) {
      expect(err.code).to.equal('ERR_NOT_FOUND')
    }
  })
})
