/* eslint-env mocha */
'use strict'

const importer = require('../src')
const exporter = require('ipfs-unixfs-exporter')

const extend = require('deep-extend')
const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const spy = require('sinon/lib/sinon/spy')
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const UnixFs = require('ipfs-unixfs')
const collectLeafCids = require('./helpers/collect-leaf-cids')
const loadFixture = require('aegir/fixtures')
const bigFile = loadFixture('test/fixtures/1.2MiB.txt')
const smallFile = loadFixture('test/fixtures/200Bytes.txt')

function stringifyMh (files) {
  return files.map((file) => {
    file.cid = file.cid.toBaseEncodedString()
    return file
  })
}

const baseFiles = {
  '200Bytes.txt': {
    path: '200Bytes.txt',
    cid: 'QmQmZQxSKQppbsWfVzBvg59Cn3DKtsNVQ94bjAxg2h3Lb8',
    size: 211,
    name: '',
    leafSize: 200
  },
  '1.2MiB.txt': {
    path: '1.2MiB.txt',
    cid: 'QmbPN6CXXWpejfQgnRYnMQcVYkFHEntHWqLNQjbkatYCh1',
    size: 1328062,
    name: '',
    leafSize: 1258000
  }
}

const strategyBaseFiles = {
  flat: baseFiles,
  balanced: extend({}, baseFiles, {
    '1.2MiB.txt': {
      cid: 'QmeEGqUisUD2T6zU96PrZnCkHfXCGuQeGWKu4UoSuaZL3d',
      size: 1335420
    }
  }),
  trickle: extend({}, baseFiles, {
    '1.2MiB.txt': {
      cid: 'QmaiSohNUt1rBf2Lqz6ou54NHVPTbXbBoPuq9td4ekcBx4',
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
      cid: 'QmQ1S6eEamaf4t948etp8QiYQ9avrKCogiJnPRgNkVreLv',
      size: 1335478
    },
    pim: {
      cid: 'QmUpzaN4Jio2GB3HoPSRCMQD5EagdMWjSEGD4SGZXaCw7W',
      size: 1335744
    },
    'pam/pum': {
      cid: 'QmUpzaN4Jio2GB3HoPSRCMQD5EagdMWjSEGD4SGZXaCw7W',
      size: 1335744
    },
    pam: {
      cid: 'QmVoVD4fEWFLJLjvRCg4bGrziFhgECiaezp79AUfhuLgno',
      size: 2671269
    }
  },
  trickle: {
    'foo-big': {
      path: 'foo-big',
      cid: 'QmPh6KSS7ghTqzgWhaoCiLoHFPF7HGqUxx7q9vcM5HUN4U',
      size: 1334657
    },
    pim: {
      cid: 'QmPAn3G2x2nrq4A1fu2XUpwWtpqG4D1YXFDrU615NHvJbr',
      size: 1334923
    },
    'pam/pum': {
      cid: 'QmPAn3G2x2nrq4A1fu2XUpwWtpqG4D1YXFDrU615NHvJbr',
      size: 1334923
    },
    pam: {
      cid: 'QmZTJah1xpG9X33ZsPtDEi1tYSHGDqQMRHsGV5xKzAR2j4',
      size: 2669627
    }
  }
}

const checkLeafNodeTypes = async (ipld, options, expected, done) => {
  const files = []

  for await (const file of importer([{
    path: '/foo',
    content: Buffer.alloc(262144 + 5).fill(1)
  }], ipld, options)) {
    files.push(file)
  }

  const node = await ipld.get(files[0].cid)
  const meta = UnixFs.unmarshal(node.data)

  expect(meta.type).to.equal('file')
  expect(node.links.length).to.equal(2)

  const linkedNodes = await Promise.all(
    node.links.map(link => ipld.get(link.cid))
  )

  linkedNodes.forEach(node => {
    const meta = UnixFs.unmarshal(node.data)
    expect(meta.type).to.equal(expected)
  })
}

const checkNodeLinks = async (ipld, options, expected) => {
  for await (const file of importer([{
    path: '/foo',
    content: Buffer.alloc(100).fill(1)
  }], ipld, options)) {
    const node = await ipld.get(file.cid)
    const meta = UnixFs.unmarshal(node.data)

    expect(meta.type).to.equal('file')
    expect(node.links.length).to.equal(expected)
  }
}

strategies.forEach((strategy) => {
  const baseFiles = strategyBaseFiles[strategy]
  const defaultResults = extend({}, baseFiles, {
    'foo/bar/200Bytes.txt': extend({}, baseFiles['200Bytes.txt'], {
      path: 'foo/bar/200Bytes.txt'
    }),
    foo: {
      path: 'foo',
      cid: 'QmQrb6KKWGo8w7zKfx2JksptY6wN7B2ysSBdKZr4xMU36d',
      size: 320
    },
    'foo/bar': {
      path: 'foo/bar',
      cid: 'Qmf5BQbTUyUAvd6Ewct83GYGnE1F6btiC3acLhR8MDxgkD',
      size: 270
    },
    'foo-big/1.2MiB.txt': extend({}, baseFiles['1.2MiB.txt'], {
      path: 'foo-big/1.2MiB.txt'
    }),
    'foo-big': {
      path: 'foo-big',
      cid: 'Qma6JU3FoXU9eAzgomtmYPjzFBwVc2rRbECQpmHFiA98CJ',
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
      cid: 'QmNk8VPGb3fkAQgoxctXo4Wmnr4PayFTASy4MiVXTtXqiA',
      size: 1328386
    },
    'empty-dir': {
      path: 'empty-dir',
      cid: 'QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn',
      size: 4
    },
    'pam/pum': {
      cid: 'QmNk8VPGb3fkAQgoxctXo4Wmnr4PayFTASy4MiVXTtXqiA',
      size: 1328386
    },
    pam: {
      cid: 'QmPAixYTaYnPe795fcWcuRpo6tfwHgRKNiBHpMzoomDVN6',
      size: 2656553
    },
    '200Bytes.txt with raw leaves': extend({}, baseFiles['200Bytes.txt'], {
      cid: 'zb2rhXrz1gkCv8p4nUDZRohY6MzBE9C3HVTVDP72g6Du3SD9Q',
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
      }
    }

    before((done) => {
      inMemory(IPLD, (err, resolver) => {
        expect(err).to.not.exist()

        ipld = resolver

        done()
      })
    })

    it('fails on bad input', async () => {
      try {
        for await (const _ of importer([{ // eslint-disable-line no-unused-vars
          path: '200Bytes.txt',
          content: 'banana'
        }], ipld, options)) {
          // ...falala
        }

        throw new Error('No error was thrown')
      } catch (err) {
        expect(err.code).to.equal('EINVALIDCONTENT')
      }
    })

    it('survives bad progress option', async () => {
      let file

      for await (const f of importer([{
        path: '200Bytes.txt',
        content: Buffer.from([0, 1, 2])
      }], ipld, {
        ...options,
        progress: null
      })) {
        file = f
      }

      expect(file).to.be.ok()
    })

    it('doesn\'t yield anything on empty source', async () => {
      const files = []

      for await (const file of importer([], ipld, options)) {
        files.push(file)
      }

      expect(files).to.be.empty()
    })

    it('doesn\'t yield anything on empty file', async () => {
      const files = []

      for await (const file of importer([{
        path: 'emptyfile',
        content: Buffer.alloc(0)
      }], ipld, options)) {
        files.push(file)
      }

      expect(files.length).to.eql(1)

      // always yield empty node
      expect(files[0].cid.toBaseEncodedString()).to.eql('QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH')
    })

    it('fails on more than one root', async () => {
      try {
        for await (const _ of importer([{ // eslint-disable-line no-unused-vars
          path: '/beep/200Bytes.txt',
          content: smallFile
        }, {
          path: '/boop/200Bytes.txt',
          content: bigFile
        }], ipld, options)) {
          // ...falala
        }

        throw new Error('No error was thrown')
      } catch (err) {
        expect(err.code).to.equal('EMORETHANONEROOT')
      }
    })

    it('small file with an escaped slash in the title', async () => {
      const filePath = `small-\\/file-${Math.random()}.txt`
      const files = []

      for await (const file of importer([{
        path: filePath,
        content: smallFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(files.length).to.equal(1)
      expect(files[0].path).to.equal(filePath)
    })

    it('small file with square brackets in the title', async () => {
      const filePath = `small-[v]-file-${Math.random()}.txt`
      const files = []

      for await (const file of importer([{
        path: filePath,
        content: smallFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(files.length).to.equal(1)
      expect(files[0].path).to.equal(filePath)
    })

    it('small file (smaller than a chunk)', async () => {
      const files = []

      for await (const file of importer([{
        path: '200Bytes.txt',
        content: smallFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt']])
    })

    it('small file (smaller than a chunk) with raw leaves', async () => {
      const files = []

      for await (const file of importer([{
        path: '200Bytes.txt',
        content: smallFile
      }], ipld, {
        ...options,
        rawLeaves: true
      })) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt with raw leaves']])
    })

    it('small file as buffer (smaller than a chunk)', async () => {
      const files = []

      for await (const file of importer([{
        path: '200Bytes.txt',
        content: smallFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.be.eql([expected['200Bytes.txt']])
    })

    it('small file (smaller than a chunk) inside a dir', async () => {
      const files = []

      for await (const file of importer([{
        path: 'foo/bar/200Bytes.txt',
        content: smallFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(files.length).to.equal(3)
      stringifyMh(files).forEach((file) => {
        expect(file).to.deep.equal(expected[file.path])
      })
    })

    it('file bigger than a single chunk', async () => {
      this.timeout(60 * 1000)

      const files = []

      for await (const file of importer([{
        path: '1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.be.eql([expected['1.2MiB.txt']])
    })

    it('file bigger than a single chunk inside a dir', async () => {
      this.timeout(60 * 1000)

      const files = []

      for await (const file of importer([{
        path: 'foo-big/1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.deep.equal([
        expected['foo-big/1.2MiB.txt'],
        expected['foo-big']
      ])
    })

    it('empty directory', async () => {
      const files = []

      for await (const file of importer([{
        path: 'empty-dir'
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).to.be.eql([expected['empty-dir']])
    })

    it('directory with files', async () => {
      const files = []

      for await (const file of importer([{
        path: 'pim/200Bytes.txt',
        content: smallFile
      }, {
        path: 'pim/1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        files.push(file)
      }

      expect(stringifyMh(files)).be.eql([
        expected['pim/200Bytes.txt'],
        expected['pim/1.2MiB.txt'],
        expected.pim]
      )
    })

    it('nested directory (2 levels deep)', async () => {
      const files = []

      for await (const file of importer([{
        path: 'pam/pum/200Bytes.txt',
        content: smallFile
      }, {
        path: 'pam/pum/1.2MiB.txt',
        content: bigFile
      }, {
        path: 'pam/1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        files.push(file)
      }

      stringifyMh(files).forEach(eachFile)

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

    it('will not write to disk if passed "onlyHash" option', async () => {
      const content = String(Math.random() + Date.now())
      const files = []

      for await (const file of importer([{
        path: content + '.txt',
        content: Buffer.from(content)
      }], ipld, {
        onlyHash: true
      })) {
        files.push(file)
      }

      const file = files[0]
      expect(file).to.exist()

      try {
        await ipld.get(file.cid)

        throw new Error('No error was thrown')
      } catch (err) {
        expect(err.code).to.equal('ERR_NOT_FOUND')
      }
    })

    it('will call an optional progress function', async () => {
      const maxChunkSize = 2048

      const options = {
        progress: spy(),
        chunkerOptions: {
          maxChunkSize
        }
      }

      for await (const _ of importer([{ // eslint-disable-line no-unused-vars
        path: '1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        // falala
      }

      expect(options.progress.called).to.equal(true)
      expect(options.progress.args[0][0]).to.equal(maxChunkSize)
    })

    it('will import files with CID version 1', async () => {
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

      const files = []

      // Pass a copy of inputFiles, since the importer mutates them
      for await (const file of importer(inputFiles.map(f => Object.assign({}, f)), ipld, options)) {
        files.push(file)
      }

      const file = files[0]
      expect(file).to.exist()

      for (let i = 0; i < file.length; i++) {
        const file = files[i]

        const cid = file.cid.toV1()
        const inputFile = inputFiles.find(f => f.path === file.path)

        // Just check the intermediate directory can be retrieved
        if (!inputFile) {
          await ipld.get(cid)
        }

        // Check the imported content is correct
        const node = await exporter(cid, ipld)
        const chunks = []

        for await (const chunk of node.content()) {
          chunks.push(chunk)
        }

        expect(Buffer.concat(chunks)).to.deep.equal(inputFile.content)
      }
    })

    it('imports file with raw leaf nodes when specified', async () => {
      return checkLeafNodeTypes(ipld, {
        leafType: 'raw'
      }, 'raw')
    })

    it('imports file with file leaf nodes when specified', async () => {
      return checkLeafNodeTypes(ipld, {
        leafType: 'file'
      }, 'file')
    })

    it('reduces file to single node when specified', async () => {
      return checkNodeLinks(ipld, {
        reduceSingleLeafToSelf: true
      }, 0)
    })

    it('does not reduce file to single node when overidden by options', async () => {
      return checkNodeLinks(ipld, {
        reduceSingleLeafToSelf: false
      }, 1)
    })

    it('uses raw leaf nodes when requested', async () => {
      this.timeout(60 * 1000)

      const options = {
        rawLeaves: true
      }

      for await (const file of importer([{
        path: '1.2MiB.txt',
        content: bigFile
      }], ipld, options)) {
        for await (const { cid } of collectLeafCids(file.cid, ipld)) {
          expect(cid.codec).to.be('raw')
          expect(cid.version).to.be(1)
        }
      }
    })
  })
})
