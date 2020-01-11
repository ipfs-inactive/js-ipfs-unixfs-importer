/* eslint-env mocha */
'use strict'

const importer = require('../src')

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const CID = require('cids')

const fromPartsTest = (iter, size) => async () => {
  const content = iter()
  const inmem = await inMemory(IPLD)
  const sizes = [size]
  const ipld = {
    put: (node, ...args) => {
      expect(node.toJSON().size).to.equal(sizes.shift())
      return inmem.put(node, ...args)
    }
  }
  for await (const part of importer([{ path: 'test', content }], ipld, { fromParts: true })) {
    expect(part.size).to.equal(size)
  }
  expect(sizes).to.be.empty()
}

describe('custom chunker', function () {
  // eslint bug https://github.com/eslint/eslint/issues/12459
  // eslint-disable-next-line require-await
  const multi = async function * () {
    yield { size: 138102, cid: new CID('mAVUSIO7K3sMLqZPsJ/6SYMa5HiHBaj81xjniNYRUXbpKl/Ac') }
    yield { size: 138102, cid: new CID('mAVUSIO7K3sMLqZPsJ/6SYMa5HiHBaj81xjniNYRUXbpKl/Ac') }
  }
  it('works with multiple parts', fromPartsTest(multi, 276312))

  // eslint-disable-next-line require-await
  const single = async function * () {
    yield { size: 138102, cid: new CID('mAVUSIO7K3sMLqZPsJ/6SYMa5HiHBaj81xjniNYRUXbpKl/Ac') }
  }
  it('works with single part', fromPartsTest(single, 138160))
})
