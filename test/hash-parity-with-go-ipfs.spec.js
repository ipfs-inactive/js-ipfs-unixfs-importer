/* eslint-env mocha */
'use strict'

const importer = require('../src')

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const randomByteStream = require('./helpers/finite-pseudorandom-byte-stream')
const first = require('async-iterator-first')

const strategies = [
  'flat',
  'trickle',
  'balanced'
]

const expectedHashes = {
  flat: 'QmRgXEDv6DL8uchf7h9j8hAGG8Fq5r1UZ6Jy3TQAPxEb76',
  balanced: 'QmVY1TFpjYKSo8LRG9oYgH4iy9AduwDvBGNhqap1Gkxme3',
  trickle: 'QmYPsm9oVGjWECkT7KikZmrf8imggqKe8uS8Jco3qfWUCH'
}

strategies.forEach(strategy => {
  const options = {
    strategy: strategy
  }

  describe('go-ipfs interop using importer:' + strategy, () => {
    let ipld

    before((done) => {
      inMemory(IPLD, (err, resolver) => {
        expect(err).to.not.exist()

        ipld = resolver

        done()
      })
    })

    it('yields the same tree as go-ipfs', async function () {
      this.timeout(10 * 1000)

      const source = [{
        path: 'big.dat',
        content: randomByteStream(45900000, 7382)
      }]

      const file = await first(importer(source, ipld, options))

      expect(file.cid.toBaseEncodedString()).to.be.equal(expectedHashes[strategy])
    })
  })
})
