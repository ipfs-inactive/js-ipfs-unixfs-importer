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
    multihash: 'QmQmZQxSKQppbsWfVzBvg59Cn3DKtsNVQ94bjAxg2h3Lb8',
    size: 211,
    name: '',
    leafSize: 200
  },
  '1.2MiB.txt': {
    path: '1.2MiB.txt',
    multihash: 'QmbPN6CXXWpejfQgnRYnMQcVYkFHEntHWqLNQjbkatYCh1',
    size: 1328062,
    name: '',
    leafSize: 1258000
  }
}

const strategyBaseFiles = {
  flat: baseFiles,
  balanced: extend({}, baseFiles, {
    '1.2MiB.txt': {
      multihash: 'QmeEGqUisUD2T6zU96PrZnCkHfXCGuQeGWKu4UoSuaZL3d',
      size: 1335420
    }
  }),
  trickle: extend({}, baseFiles, {
    '1.2MiB.txt': {
      multihash: 'QmaiSohNUt1rBf2Lqz6ou54NHVPTbXbBoPuq9td4ekcBx4',
      size: 1334599
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
      multihash: 'QmQ1S6eEamaf4t948etp8QiYQ9avrKCogiJnPRgNkVreLv',
      size: 1335478
    },
    pim: {
      multihash: 'QmUpzaN4Jio2GB3HoPSRCMQD5EagdMWjSEGD4SGZXaCw7W',
      size: 1335744
    },
    'pam/pum': {
      multihash: 'QmUpzaN4Jio2GB3HoPSRCMQD5EagdMWjSEGD4SGZXaCw7W',
      size: 1335744
    },
    pam: {
      multihash: 'QmVoVD4fEWFLJLjvRCg4bGrziFhgECiaezp79AUfhuLgno',
      size: 2671269
    }
  },
  trickle: {
    'foo-big': {
      path: 'foo-big',
      multihash: 'QmPh6KSS7ghTqzgWhaoCiLoHFPF7HGqUxx7q9vcM5HUN4U',
      size: 1334657
    },
    pim: {
      multihash: 'QmPAn3G2x2nrq4A1fu2XUpwWtpqG4D1YXFDrU615NHvJbr',
      size: 1334923
    },
    'pam/pum': {
      multihash: 'QmPAn3G2x2nrq4A1fu2XUpwWtpqG4D1YXFDrU615NHvJbr',
      size: 1334923
    },
    pam: {
      multihash: 'QmZTJah1xpG9X33ZsPtDEi1tYSHGDqQMRHsGV5xKzAR2j4',
      size: 2669627
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
        multihash: 'QmQrb6KKWGo8w7zKfx2JksptY6wN7B2ysSBdKZr4xMU36d',
        size: 320
      },
      'foo/bar': {
        path: 'foo/bar',
        multihash: 'Qmf5BQbTUyUAvd6Ewct83GYGnE1F6btiC3acLhR8MDxgkD',
        size: 270
      },
      'foo-big/1.2MiB.txt': extend({}, baseFiles['1.2MiB.txt'], {
        path: 'foo-big/1.2MiB.txt'
      }),
      'foo-big': {
        path: 'foo-big',
        multihash: 'Qma6JU3FoXU9eAzgomtmYPjzFBwVc2rRbECQpmHFiA98CJ',
        size: 1328120
      },
      'pim/200Bytes.txt': extend({}, baseFiles['200Bytes.txt'], {
        path: 'pim/200Bytes.txt'
      }),
      'pim/1.2MiB.txt': extend({}, baseFiles['1.2MiB.txt'], {
        path: 'pim/1.2MiB.txt'
      }),
      pim: {
        path: 'pim',
        multihash: 'QmNk8VPGb3fkAQgoxctXo4Wmnr4PayFTASy4MiVXTtXqiA',
        size: 1328386
      },
      'empty-dir': {
        path: 'empty-dir',
        multihash: 'QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn',
        size: 4
      },
      'pam/pum': {
        multihash: 'QmNk8VPGb3fkAQgoxctXo4Wmnr4PayFTASy4MiVXTtXqiA',
        size: 1328386
      },
      pam: {
        multihash: 'QmPAixYTaYnPe795fcWcuRpo6tfwHgRKNiBHpMzoomDVN6',
        size: 2656553
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
        }
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
            expect(new CID(nodes[0].multihash).toBaseEncodedString()).to.be.eql('QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH')
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
