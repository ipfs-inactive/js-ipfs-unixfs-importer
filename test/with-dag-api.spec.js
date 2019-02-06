/* eslint-env mocha */
/* eslint max-nested-callbacks: ["error", 8] */

'use strict'

const importer = require('./../src')

const extend = require('deep-extend')
const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const empty = require('pull-stream/sources/empty')
const onEnd = require('pull-stream/sinks/on-end')
const collect = require('pull-stream/sinks/collect')
const loadFixture = require('aegir/fixtures')
const CID = require('cids')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')

function stringifyMh (files) {
  return files.map((file) => {
    file.multihash = new CID(file.multihash).toBaseEncodedString()
    return file
  })
}

const bigFile = loadFixture('test/fixtures/1.2MiB.txt')
const smallFile = loadFixture('test/fixtures/200Bytes.txt')

const baseFiles = {
  '200Bytes.txt': {
    path: '200Bytes.txt',
    multihash: 'bafybeibeddt74r7hfsred3rupoyk7t3yqrnc6lq2tofkbjyeancgae7yc4',
    size: 211,
    name: '',
    leafSize: 200
  },
  '1.2MiB.txt': {
    path: '1.2MiB.txt',
    multihash: 'bafybeidye343eoqqq6dbz75kytgafi4jwm2c7tybdlj2xvnj5epthnofmm',
    size: 1330520,
    name: '',
    leafSize: 1258000
  }
}

const strategyBaseFiles = {
  flat: baseFiles,
  balanced: extend({}, baseFiles, {
    '1.2MiB.txt': {
      multihash: 'bafybeiaxcek2b4eedgyx4jjh4gquaof4beqemzz5ch4rmlsjsr67soz55q',
      size: 1338154
    }
  }),
  trickle: extend({}, baseFiles, {
    '1.2MiB.txt': {
      multihash: 'bafybeicsszy6evjc47ldfasv2kh5wydapatazcuantf6nf6iwywftvggjq',
      size: 1337301
    }
  })
}

const strategies = [
  'flat',
  'balanced',
  'trickle'
]

const strategyOverrides = {
  balanced: {
    'foo-big': {
      path: 'foo-big',
      multihash: 'bafybeif7nmfq5sl7bvxi4o2kmqvpdri7l7jlgqe5xldeva2bz2xb64seky',
      size: 1338214
    },
    pim: {
      multihash: 'bafybeiaqfwmlhmlvftd7pe3d7fr2nuiwwvcd67zkh3p7u2vq6kg54sp36m',
      size: 1338482
    },
    'pam/pum': {
      multihash: 'bafybeiaqfwmlhmlvftd7pe3d7fr2nuiwwvcd67zkh3p7u2vq6kg54sp36m',
      size: 1338482
    },
    pam: {
      multihash: 'QmVoVD4fEWFLJLjvRCg4bGrziFhgECiaezp79AUfhuLgno',
      size: 2676745
    }
  },
  trickle: {
    'foo-big': {
      path: 'foo-big',
      multihash: 'bafybeiauchq6pdjaf4nchxl7gtigktflzvlyiouemk3k5zdetzpbpgsoky',
      size: 1337361
    },
    pim: {
      multihash: 'bafybeie3zigxtjsbyxpnhep74zofd5z6j52bpdzoxvptlr6syaqgxqcbui',
      size: 1337629
    },
    'pam/pum': {
      multihash: 'bafybeie3zigxtjsbyxpnhep74zofd5z6j52bpdzoxvptlr6syaqgxqcbui',
      size: 1337629
    },
    pam: {
      multihash: 'QmZTJah1xpG9X33ZsPtDEi1tYSHGDqQMRHsGV5xKzAR2j4',
      size: 2675039
    }
  }

}

describe('with dag-api', function () {
  strategies.forEach(strategy => {
    const baseFiles = strategyBaseFiles[strategy]
    const defaultResults = extend({}, baseFiles, {
      'foo/bar/200Bytes.txt': extend({}, baseFiles['200Bytes.txt'], {
        path: 'foo/bar/200Bytes.txt'
      }),
      foo: {
        path: 'foo',
        multihash: 'bafybeiahadqxdlbf2w5l3bnyjmrso3zwlzeg6vcft7n6ogc3kgeg3eqcne',
        size: 324
      },
      'foo/bar': {
        path: 'foo/bar',
        multihash: 'bafybeif2tllv3roheq754k4kbmlx3w6e36m3vntwe4apzufwd3aaqeqi5u',
        size: 272
      },
      'foo-big/1.2MiB.txt': extend({}, baseFiles['1.2MiB.txt'], {
        path: 'foo-big/1.2MiB.txt'
      }),
      'foo-big': {
        path: 'foo-big',
        multihash: 'bafybeifmrcnmuli3vqni2gh5bzhq6dvn22kkpju6lak4xxjv7ut5m4qjxe',
        size: 1330580
      },
      'pim/200Bytes.txt': extend({}, baseFiles['200Bytes.txt'], {
        path: 'pim/200Bytes.txt'
      }),
      'pim/1.2MiB.txt': extend({}, baseFiles['1.2MiB.txt'], {
        path: 'pim/1.2MiB.txt'
      }),
      pim: {
        path: 'pim',
        multihash: 'bafybeih63r7unrizaqfpq6z337u3jh3m22ona552hpyqhdst3ign4ncnca',
        size: 1330848
      },
      'empty-dir': {
        path: 'empty-dir',
        multihash: 'bafybeiczsscdsbs7ffqz55asqdf3smv6klcw3gofszvwlyarci47bgf354',
        size: 4
      },
      'pam/pum': {
        multihash: 'bafybeih63r7unrizaqfpq6z337u3jh3m22ona552hpyqhdst3ign4ncnca',
        size: 1330848
      },
      pam: {
        multihash: 'QmPAixYTaYnPe795fcWcuRpo6tfwHgRKNiBHpMzoomDVN6',
        size: 2661477
      }
    }, strategyOverrides[strategy])

    const expected = extend({}, defaultResults, strategies[strategy])

    describe('importer: ' + strategy, function () {
      this.timeout(50 * 1000)

      let dag

      const options = {
        strategy: strategy,
        maxChildrenPerNode: 10,
        chunkerOptions: {
          maxChunkSize: 1024
        },
        rawLeaves: false
      }

      before(function (done) {
        inMemory(IPLD, (err, resolver) => {
          if (err) {
            return done(err)
          }

          dag = resolver

          done()
        })
      })

      it('fails on bad input', (done) => {
        pull(
          values([{
            path: '200Bytes.txt',
            content: 'banana'
          }]),
          importer(dag, options),
          onEnd((err) => {
            expect(err).to.exist()
            done()
          })
        )
      })

      it('doesn\'t yield anything on empty source', (done) => {
        pull(
          empty(),
          importer(dag, options),
          collect((err, nodes) => {
            expect(err).to.not.exist()
            expect(nodes.length).to.be.eql(0)
            done()
          }))
      })

      it('doesn\'t yield anything on empty file', (done) => {
        pull(
          values([{
            path: 'emptyfile',
            content: empty()
          }]),
          importer(dag, options),
          collect((err, nodes) => {
            expect(err).to.not.exist()
            expect(nodes.length).to.be.eql(1)
            // always yield empty node
            expect(new CID(nodes[0].multihash).toBaseEncodedString()).to.be.eql('bafybeif7ztnhq65lumvvtr4ekcwd2ifwgm3awq4zfr3srh462rwyinlb4y')
            done()
          }))
      })

      it('fails on more than one root', (done) => {
        pull(
          values([
            {
              path: '/beep/200Bytes.txt',
              content: values([smallFile])
            },
            {
              path: '/boop/200Bytes.txt',
              content: values([smallFile])
            }
          ]),
          importer(dag, options),
          onEnd((err) => {
            expect(err).to.exist()
            expect(err.message).to.be.eql('detected more than one root')
            done()
          })
        )
      })

      it('small file (smaller than a chunk)', (done) => {
        pull(
          values([{
            path: '200Bytes.txt',
            content: values([smallFile])
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()
            expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt']])
            done()
          })
        )
      })

      it('small file as buffer (smaller than a chunk)', (done) => {
        pull(
          values([{
            path: '200Bytes.txt',
            content: smallFile
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()
            expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt']])
            done()
          })
        )
      })

      it('small file (smaller than a chunk) inside a dir', (done) => {
        pull(
          values([{
            path: 'foo/bar/200Bytes.txt',
            content: values([smallFile])
          }]),
          importer(dag, options),
          collect(collected)
        )

        function collected (err, files) {
          expect(err).to.not.exist()
          expect(files.length).to.equal(3)
          stringifyMh(files).forEach((file) => {
            if (file.path === 'foo/bar/200Bytes.txt') {
              expect(file).to.be.eql(expected['foo/bar/200Bytes.txt'])
            }
            if (file.path === 'foo') {
              expect(file).to.be.eql(expected.foo)
            }
            if (file.path === 'foo/bar') {
              expect(file).to.be.eql(expected['foo/bar'])
            }
          })
          done()
        }
      })

      it('file bigger than a single chunk', (done) => {
        pull(
          values([{
            path: '1.2MiB.txt',
            content: values([bigFile])
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()
            expect(stringifyMh(files)).to.be.eql([expected['1.2MiB.txt']])
            done()
          })
        )
      })

      it('file bigger than a single chunk inside a dir', (done) => {
        pull(
          values([{
            path: 'foo-big/1.2MiB.txt',
            content: values([bigFile])
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()

            expect(stringifyMh(files)).to.be.eql([
              expected['foo-big/1.2MiB.txt'],
              expected['foo-big']
            ])

            done()
          })
        )
      })

      it('empty directory', (done) => {
        pull(
          values([{
            path: 'empty-dir'
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()

            expect(stringifyMh(files)).to.be.eql([expected['empty-dir']])

            done()
          })
        )
      })

      it('directory with files', (done) => {
        pull(
          values([{
            path: 'pim/200Bytes.txt',
            content: values([smallFile])
          }, {
            path: 'pim/1.2MiB.txt',
            content: values([bigFile])
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()

            expect(stringifyMh(files)).be.eql([
              expected['pim/200Bytes.txt'],
              expected['pim/1.2MiB.txt'],
              expected.pim]
            )

            done()
          })
        )
      })

      it('nested directory (2 levels deep)', (done) => {
        pull(
          values([{
            path: 'pam/pum/200Bytes.txt',
            content: values([smallFile])
          }, {
            path: 'pam/pum/1.2MiB.txt',
            content: values([bigFile])
          }, {
            path: 'pam/1.2MiB.txt',
            content: values([bigFile])
          }]),
          importer(dag, options),
          collect((err, files) => {
            expect(err).to.not.exist()

            // need to sort as due to parallel storage the order can vary
            stringifyMh(files).forEach(eachFile)

            done()
          })
        )

        function eachFile (file) {
          if (file.path === 'pam/pum/200Bytes.txt') {
            expect(file.cid).to.be.eql(expected['200Bytes.txt'].cid)
            expect(file.size).to.be.eql(expected['200Bytes.txt'].size)
          }
          if (file.path === 'pam/pum/1.2MiB.txt') {
            expect(file.cid).to.be.eql(expected['1.2MiB.txt'].cid)
            expect(file.size).to.be.eql(expected['1.2MiB.txt'].size)
          }
          if (file.path === 'pam/pum') {
            const dir = expected['pam/pum']
            expect(file.cid).to.be.eql(dir.cid)
            expect(file.size).to.be.eql(dir.size)
          }
          if (file.path === 'pam/1.2MiB.txt') {
            expect(file.cid).to.be.eql(expected['1.2MiB.txt'].cid)
            expect(file.size).to.be.eql(expected['1.2MiB.txt'].size)
          }
          if (file.path === 'pam') {
            const dir = expected.pam
            expect(file.cid).to.be.eql(dir.cid)
            expect(file.size).to.be.eql(dir.size)
          }
        }
      })
    })
  })
})
