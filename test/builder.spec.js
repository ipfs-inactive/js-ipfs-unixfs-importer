/* eslint-env mocha */
'use strict'

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const mh = require('multihashes')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const UnixFS = require('ipfs-unixfs')
const builder = require('../src/dag-builder')
const first = require('async-iterator-first')

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
  const opts = {
    strategy: 'flat',
    chunker: 'fixed',
    leafType: 'file',
    reduceSingleLeafToSelf: true,
    format: 'dag-pb',
    hashAlg: 'sha2-256',
    progress: () => {},
    chunkerOptions: {
      maxChunkSize: 262144
    }
  }

  it('allows multihash hash algorithm to be specified', async () => {
    for (let i = 0; i < testMultihashes.length; i++) {
      const hashAlg = testMultihashes[i]
      const options = {
        ...opts,
        hashAlg
      }
      const content = String(Math.random() + Date.now())
      const inputFile = {
        path: content + '.txt',
        content: Buffer.from(content)
      }

      const imported = await first(builder([inputFile], ipld, options))

      expect(imported).to.exist()

      // Verify multihash has been encoded using hashAlg
      expect(mh.decode(imported.cid.multihash).name).to.equal(hashAlg)

      // Fetch using hashAlg encoded multihash
      const node = await ipld.get(imported.cid)

      const fetchedContent = UnixFS.unmarshal(node.Data).data
      expect(fetchedContent).to.deep.equal(inputFile.content)
    }
  })

  it('allows multihash hash algorithm to be specified for big file', async function () {
    this.timeout(30000)

    for (let i = 0; i < testMultihashes.length; i++) {
      const hashAlg = testMultihashes[i]
      const options = {
        ...opts,
        hashAlg
      }
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

      const options = {
        ...opts,
        hashAlg
      }
      const inputFile = {
        path: `${String(Math.random() + Date.now())}-dir`,
        content: null
      }

      const imported = await first(builder([Object.assign({}, inputFile)], ipld, options))

      expect(mh.decode(imported.cid.multihash).name).to.equal(hashAlg)

      // Fetch using hashAlg encoded multihash
      const node = await ipld.get(imported.cid)

      const meta = UnixFS.unmarshal(node.Data)
      expect(meta.type).to.equal('directory')
    }
  })
})
