/* eslint-env mocha */
'use strict'

const importer = require('../src')

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const collect = require('pull-stream/sinks/collect')
const CID = require('cids')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const randomByteStream = require('./helpers/finite-pseudorandom-byte-stream')

const strategies = [
  'flat',
  'trickle',
  'balanced'
]

const expectedHashes = {
  flat: 'bafybeid7e25emp6ruqhuyrse2iwxd722gvb42hr6lycievs2sllafgk4fa',
  balanced: 'bafybeiehl4ol4wqhzzxouk4lqwjykqulpg4qvg4g24gntqbsqrqfs6rocu',
  trickle: 'bafybeihc3tgk6xojwmz7zl5kuzrigwcgnkxqd6dniiemohf4zemqnsqte4'
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

    it('yields the same tree as go-ipfs', function (done) {
      this.timeout(10 * 1000)
      pull(
        values([
          {
            path: 'big.dat',
            content: randomByteStream(45900000, 7382)
          }
        ]),
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(files.length).to.be.equal(1)

          const file = files[0]
          expect(new CID(file.multihash).toBaseEncodedString()).to.be.equal(expectedHashes[strategy])
          done()
        })
      )
    })
  })
})
