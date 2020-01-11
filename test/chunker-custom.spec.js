/* eslint-env mocha */
'use strict'

const importer = require('../src')

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const spy = require('sinon/lib/sinon/spy')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const UnixFs = require('ipfs-unixfs')
const collectLeafCids = require('./helpers/collect-leaf-cids')
const loadFixture = require('aegir/fixtures')
const isNode = require('detect-node')
const bigFile = loadFixture((isNode ? __dirname : 'test') + '/fixtures/1.2MiB.txt')
const smallFile = loadFixture((isNode ? __dirname : 'test') + '/fixtures/200Bytes.txt')
const all = require('it-all')
const first = require('it-first')

const iter = async function * () {
  yield Buffer.from('one')
  yield Buffer.from('two')
}

describe('custom chunker', function () {
  it('keeps custom chunking', async () => {
    const chunker = source => source
    const content = iter()
    const inmem = await inMemory(IPLD)
    const sizes = [11, 11, 116]
    const ipld = { put: (node, ...args) => {
      expect(node.toJSON().size).to.equal(sizes.shift())
      return inmem.put(node, ...args)
    }}
    for await (const part of importer([{path: 'test', content}], ipld, {chunker})) {
      expect(part.size).to.equal(116)
    }
    expect(sizes).to.be.empty
  })
})
