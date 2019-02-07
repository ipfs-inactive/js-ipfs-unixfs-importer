/* eslint-env mocha */
'use strict'

const importer = require('../src')
const exporter = require('ipfs-unixfs-exporter')

const extend = require('deep-extend')
const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const spy = require('sinon/lib/sinon/spy')
const pull = require('pull-stream/pull')
const empty = require('pull-stream/sources/empty')
const once = require('pull-stream/sources/once')
const values = require('pull-stream/sources/values')
const collect = require('pull-stream/sinks/collect')
const onEnd = require('pull-stream/sinks/on-end')
const CID = require('cids')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const loadFixture = require('aegir/fixtures')
const each = require('async/each')
const waterfall = require('async/waterfall')
const parallel = require('async/parallel')
const UnixFs = require('ipfs-unixfs')
const collectLeafCids = require('./helpers/collect-leaf-cids')

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
      multihash: 'bafybeiheoaizzsupiyjc5t7l5cczonhf27f7zcoxsudfrphutnoomi7nxq',
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
      multihash: 'bafybeifvnr4fzxothljw62c3cjicubsgasaxu6hsc6cwvh7opu4uaoh724',
      size: 2675039
    }
  }
}

const checkLeafNodeTypes = (ipld, options, expected, done) => {
  waterfall([
    (cb) => pull(
      once({
        path: '/foo',
        content: Buffer.alloc(262144 + 5).fill(1)
      }),
      importer(ipld, options),
      collect(cb)
    ),
    (files, cb) => ipld.get(new CID(files[0].multihash), cb),
    (result, cb) => {
      const node = result.value
      const meta = UnixFs.unmarshal(node.data)

      expect(meta.type).to.equal('file')
      expect(node.links.length).to.equal(2)

      parallel(
        node.links.map(link => {
          return (done) => {
            waterfall([
              (next) => ipld.get(link.cid, next),
              (result, next) => {
                const node = result.value
                const meta = UnixFs.unmarshal(node.data)

                expect(meta.type).to.equal(expected)

                next()
              }
            ], done)
          }
        }), cb)
    }
  ], done)
}

const checkNodeLinks = (ipld, options, expected, done) => {
  waterfall([
    (cb) => pull(
      once({
        path: '/foo',
        content: Buffer.alloc(100).fill(1)
      }),
      importer(ipld, options),
      collect(cb)
    ),
    (files, cb) => ipld.get(new CID(files[0].multihash), cb),
    (result, cb) => {
      const node = result.value
      const meta = UnixFs.unmarshal(node.data)

      expect(meta.type).to.equal('file')
      expect(node.links.length).to.equal(expected)

      cb()
    }
  ], done)
}

strategies.forEach((strategy) => {
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
      multihash: 'bafybeid2cwow7m4ii3523lzmippdhxpmc4qvmaezaftbo54xui662sidoa',
      size: 2661477
    },
    '200Bytes.txt with raw leaves': extend({}, baseFiles['200Bytes.txt'], {
      multihash: 'bafkreiasdth246e2lufrhjpkdvud6yci622ouqgss3wou35reukddzlut4',
      size: 200
    })
  }, strategyOverrides[strategy])

  const expected = extend({}, defaultResults, strategies[strategy])

  describe('importer: ' + strategy, function () {
    this.timeout(30 * 1000)

    let ipld
    const options = {
      strategy: strategy,
      maxChildrenPerNode: 10,
      chunkerOptions: {
        maxChunkSize: 1024
      },
      rawLeaves: false
    }

    before((done) => {
      inMemory(IPLD, (err, resolver) => {
        expect(err).to.not.exist()

        ipld = resolver

        done()
      })
    })

    it('fails on bad input', (done) => {
      pull(
        values([{
          path: '200Bytes.txt',
          content: 'banana'
        }]),
        importer(ipld, options),
        onEnd((err) => {
          expect(err).to.exist()
          done()
        })
      )
    })

    it('survives bad progress option', (done) => {
      pull(
        values([{
          path: '200Bytes.txt',
          content: Buffer.from([0, 1, 2])
        }]),
        importer(ipld, {
          ...options,
          progress: null
        }),
        onEnd((err) => {
          expect(err).to.not.exist()
          done()
        })
      )
    })

    it('doesn\'t yield anything on empty source', (done) => {
      pull(
        empty(),
        importer(ipld, options),
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
        importer(ipld, options),
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
            content: values([bigFile])
          }
        ]),
        importer(ipld, options),
        onEnd((err) => {
          expect(err).to.exist()
          expect(err.message).to.be.eql('detected more than one root')
          done()
        })
      )
    })

    it('small file with an escaped slash in the title', (done) => {
      const filePath = `small-\\/file-${Math.random()}.txt`

      pull(
        values([{
          path: filePath,
          content: values([smallFile])
        }]),
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(files.length).to.equal(1)
          expect(files[0].path).to.equal(filePath)
          done()
        })
      )
    })

    it('small file with square brackets in the title', (done) => {
      const filePath = `small-[v]-file-${Math.random()}.txt`

      pull(
        values([{
          path: filePath,
          content: values([smallFile])
        }]),
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(files.length).to.equal(1)
          expect(files[0].path).to.equal(filePath)
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
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt']])
          done()
        })
      )
    })

    it('small file (smaller than a chunk) with raw leaves', (done) => {
      pull(
        values([{
          path: '200Bytes.txt',
          content: values([smallFile])
        }]),
        importer(ipld, Object.assign({}, options, { rawLeaves: true })),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt with raw leaves']])
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
        importer(ipld, options),
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
        importer(ipld, options),
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

    it('file bigger than a single chunk', function (done) {
      this.timeout(60 * 1000)
      pull(
        values([{
          path: '1.2MiB.txt',
          content: values([bigFile])
        }]),
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()
          expect(stringifyMh(files)).to.be.eql([expected['1.2MiB.txt']])
          done()
        })
      )
    })

    it('file bigger than a single chunk inside a dir', function (done) {
      this.timeout(60 * 1000)
      pull(
        values([{
          path: 'foo-big/1.2MiB.txt',
          content: values([bigFile])
        }]),
        importer(ipld, options),
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
        importer(ipld, options),
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
        importer(ipld, options),
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
        importer(ipld, options),
        collect((err, files) => {
          expect(err).to.not.exist()

          // need to sort as due to parallel storage the order
          // can vary
          stringifyMh(files).forEach(eachFile)

          done()
        })
      )

      function eachFile (file) {
        if (file.path === 'pam/pum/200Bytes.txt') {
          expect(file.multihash).to.be.eql(expected['200Bytes.txt'].multihash)
          expect(file.size).to.be.eql(expected['200Bytes.txt'].size)
        }
        if (file.path === 'pam/pum/1.2MiB.txt') {
          expect(file.multihash).to.be.eql(expected['1.2MiB.txt'].multihash)
          expect(file.size).to.be.eql(expected['1.2MiB.txt'].size)
        }
        if (file.path === 'pam/pum') {
          const dir = expected['pam/pum']
          expect(file.multihash).to.be.eql(dir.multihash)
          expect(file.size).to.be.eql(dir.size)
        }
        if (file.path === 'pam/1.2MiB.txt') {
          expect(file.multihash).to.be.eql(expected['1.2MiB.txt'].multihash)
          expect(file.size).to.be.eql(expected['1.2MiB.txt'].size)
        }
        if (file.path === 'pam') {
          const dir = expected.pam
          expect(file.multihash).to.be.eql(dir.multihash)
          expect(file.size).to.be.eql(dir.size)
        }
      }
    })

    it('will not write to disk if passed "onlyHash" option', (done) => {
      const content = String(Math.random() + Date.now())
      const inputFile = {
        path: content + '.txt',
        content: Buffer.from(content)
      }

      const options = {
        onlyHash: true
      }

      const onCollected = (err, files) => {
        if (err) return done(err)

        const file = files[0]
        expect(file).to.exist()

        ipld.get(new CID(file.multihash), (err) => {
          expect(err).to.exist()
          done()
        })
      }

      pull(
        values([inputFile]),
        importer(ipld, options),
        collect(onCollected)
      )
    })

    it('will call an optional progress function', (done) => {
      options.progress = spy()

      pull(
        values([{
          path: '1.2MiB.txt',
          content: values([bigFile])
        }]),
        importer(ipld, options),
        collect(() => {
          expect(options.progress.called).to.equal(true)
          expect(options.progress.args[0][0]).to.equal(1024)
          done()
        })
      )
    })

    it('will import files with CID version 0', (done) => {
      pull(
        values([{
          path: `TEST${Date.now()}.txt`,
          content: values([Buffer.from('TEST' + Date.now())])
        }]),
        importer(ipld, { ...options, cidVersion: 0 }),
        collect((err, results) => {
          expect(err).to.not.exist()
          console.log(new CID(results[0].multihash).toString())
          expect(new CID(results[0].multihash).version).to.equal(0)
          done()
        })
      )
    })

    it('will import files with CID version 1', (done) => {
      const createInputFile = (path, size) => {
        const name = String(Math.random() + Date.now())
        path = path[path.length - 1] === '/' ? path : path + '/'
        return {
          path: path + name + '.txt',
          content: Buffer.alloc(size).fill(1)
        }
      }

      const inputFiles = [
        createInputFile('/foo', 10),
        createInputFile('/foo', 60),
        createInputFile('/foo/bar', 78),
        createInputFile('/foo/baz', 200),
        // Bigger than maxChunkSize
        createInputFile('/foo', 262144 + 45),
        createInputFile('/foo/bar', 262144 + 134),
        createInputFile('/foo/bar', 262144 + 79),
        createInputFile('/foo/bar', 262144 + 876),
        createInputFile('/foo/bar', 262144 + 21)
      ]

      const options = {
        cidVersion: 1,
        // Ensures we use DirSharded for the data below
        shardSplitThreshold: 3
      }

      const onCollected = (err, files) => {
        if (err) return done(err)

        const file = files[0]
        expect(file).to.exist()

        each(files, (file, cb) => {
          const cid = new CID(file.multihash).toV1()
          const inputFile = inputFiles.find(f => f.path === file.path)

          // Just check the intermediate directory can be retrieved
          if (!inputFile) {
            return ipld.get(cid, cb)
          }

          // Check the imported content is correct
          pull(
            exporter(cid, ipld),
            collect((err, nodes) => {
              expect(err).to.not.exist()
              pull(
                nodes[0].content,
                collect((err, chunks) => {
                  expect(err).to.not.exist()
                  expect(Buffer.concat(chunks)).to.deep.equal(inputFile.content)
                  cb()
                })
              )
            })
          )
        }, done)
      }

      pull(
        // Pass a copy of inputFiles, since the importer mutates them
        values(inputFiles.map(f => Object.assign({}, f))),
        importer(ipld, options),
        collect(onCollected)
      )
    })

    it('imports file with raw leaf nodes when specified', (done) => {
      checkLeafNodeTypes(ipld, {
        rawLeaves: false,
        leafType: 'raw'
      }, 'raw', done)
    })

    it('imports file with file leaf nodes when specified', (done) => {
      checkLeafNodeTypes(ipld, {
        rawLeaves: false,
        leafType: 'file'
      }, 'file', done)
    })

    it('reduces file to single node when specified', (done) => {
      checkNodeLinks(ipld, {
        rawLeaves: false,
        reduceSingleLeafToSelf: true
      }, 0, done)
    })

    it('does not reduce file to single node when overidden by options', (done) => {
      checkNodeLinks(ipld, {
        rawLeaves: false,
        reduceSingleLeafToSelf: false
      }, 1, done)
    })

    it('uses raw leaf nodes when requested', (done) => {
      this.timeout(60 * 1000)

      options.rawLeaves = true

      pull(
        values([{
          path: '1.2MiB.txt',
          content: values([bigFile])
        }]),
        importer(ipld, options),
        collect((error, files) => {
          expect(error).to.not.exist()

          const node = files[0]

          collectLeafCids(ipld, node.multihash, (error, cids) => {
            expect(error).to.be.not.ok()

            const rawNodes = cids
              .filter(cid => cid.codec === 'raw')

            expect(rawNodes).to.not.be.empty()

            rawNodes
              .forEach(cid => {
                expect(cid.version).to.equal(1)
              })

            done()
          })
        })
      )
    })
  })
})
