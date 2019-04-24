/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const mh = require('multihashes')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const UnixFS = require('ipfs-unixfs')
const createBuilder = require('../src/builder')
const FixedSizeChunker = require('../src/chunker/fixed-size')
const toIterator = require('pull-stream-to-async-iterator')
const first = require('async-iterator-first')

const builder = (source, ipld, options) => {
  return toIterator(
    pull(
      values(source),
      createBuilder(FixedSizeChunker, ipld, options)
    )
  )
}

describe('builder', () => {
  let ipld

  before((done) => {
    inMemory(IPLD, (err, resolver) => {
      expect(err).to.not.exist()

      ipld = resolver

      done()
    })
  })

  const testMultihashes = Object.keys(mh.names).slice(1, 40)

  it('allows multihash hash algorithm to be specified', async () => {
    for (let i = 0; i < testMultihashes.length; i++) {
      const hashAlg = testMultihashes[i]
      const options = { hashAlg, strategy: 'flat' }
      const content = String(Math.random() + Date.now())
      const inputFile = {
        path: content + '.txt',
        content: Buffer.from(content)
      }

      const imported = await first(builder([Object.assign({}, inputFile)], ipld, options))

      expect(imported).to.exist()

      // Verify multihash has been encoded using hashAlg
      expect(mh.decode(imported.cid.multihash).name).to.equal(hashAlg)

      // Fetch using hashAlg encoded multihash
      const node = await ipld.get(imported.cid)

      const fetchedContent = UnixFS.unmarshal(node.data).data
      expect(fetchedContent.equals(inputFile.content)).to.be.true()
    }
  })

  it('allows multihash hash algorithm to be specified for big file', async function () {
    this.timeout(30000)

    for (let i = 0; i < testMultihashes.length; i++) {
      const hashAlg = testMultihashes[i]
      const options = { hashAlg, strategy: 'flat' }
      const content = String(Math.random() + Date.now())
      const inputFile = {
        path: content + '.txt',
        // Bigger than maxChunkSize
        content: Buffer.alloc(262144 + 5).fill(1)
      }

      const imported = await first(builder([Object.assign({}, inputFile)], ipld, options))

      expect(imported).to.exist()
      expect(mh.decode(imported.cid.multihash).name).to.equal(hashAlg)
    }
  })

  it('allows multihash hash algorithm to be specified for a directory', async () => {
    for (let i = 0; i < testMultihashes.length; i++) {
      const hashAlg = testMultihashes[i]

      const options = { hashAlg, strategy: 'flat' }
      const inputFile = {
        path: `${String(Math.random() + Date.now())}-dir`,
        content: null
      }

      const imported = await first(builder([Object.assign({}, inputFile)], ipld, options))

      expect(mh.decode(imported.cid.multihash).name).to.equal(hashAlg)

      // Fetch using hashAlg encoded multihash
      const node = await ipld.get(imported.cid)

      const meta = UnixFS.unmarshal(node.data)
      expect(meta.type).to.equal('directory')
    }
  })
})
