/* eslint-env mocha */
'use strict'

const createImporter = require('../src')

const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const IPLD = require('ipld')
const inMemory = require('ipld-in-memory')
const pull = require('pull-stream/pull')
const values = require('pull-stream/sources/values')
const map = require('pull-stream/throughs/map')
const collect = require('pull-stream/sinks/collect')
const pushable = require('pull-pushable')

describe('importer: flush', () => {
  let ipld

  before((done) => {
    inMemory(IPLD, (err, resolver) => {
      expect(err).to.not.exist()

      ipld = resolver

      done()
    })
  })

  it('can push a single root file and flush yields no dirs', (done) => {
    const source = pushable()
    const importer = createImporter(ipld)
    pull(
      source,
      importer,
      map(node => {
        expect(node.path).to.be.eql('a')
        return node
      }),
      collect((err, files) => {
        expect(err).to.not.exist()
        expect(files.length).to.be.eql(1)
        done()
      })
    )

    source.push({
      path: 'a',
      content: values([Buffer.from('hey')])
    })

    importer.flush((err, hash) => {
      expect(err).to.not.exist()
      expect(Buffer.isBuffer(hash)).to.be.true()
      source.end()
    })
  })

  it('can push a nested file and flush yields parent dir', (done) => {
    const source = pushable()
    const importer = createImporter(ipld)
    let count = 0
    pull(
      source,
      importer,
      map(function (node) {
        count++
        if (count === 1) {
          expect(node.path).to.be.eql('b/c')
        } else if (count === 2) {
          expect(node.path).to.be.eql('b')
        }
        return node
      }),
      collect((err, files) => {
        expect(err).to.not.exist()
        expect(count).to.be.eql(2)
        done()
      })
    )

    source.push({
      path: 'b/c',
      content: values([Buffer.from('hey')])
    })

    importer.flush((err, hash) => {
      expect(err).to.not.exist()
      expect(Buffer.isBuffer(hash)).to.be.true()
      source.end()
    })
  })

  it('can flush many times, always coherent', (done) => {
    const maxDepth = 4
    const maxEntriesPerDir = 3

    let count = 0
    const tree = { children: {}, path: '', depth: 0, yielded: true }
    let currentDir = tree

    const source = pushable()
    const importer = createImporter(ipld)

    pull(
      source,
      importer,
      map((node) => {
        count++
        markDirAsYielded(node)
        return node
      }),
      collect((err, files) => {
        expect(err).to.not.exist()
        expect(count).to.be.eql(2)
        done()
      })
    )

    pushAndFlush()

    function pushAndFlush () {
      const childCount = Object.keys(currentDir.children).length
      const newDirName = childCount.toString()
      const dirPath = currentDir.path + (currentDir.depth > 0 ? '/' : '') + newDirName
      const newDir = {
        children: {},
        path: dirPath,
        depth: currentDir.depth + 1,
        yielded: false,
        parent: currentDir
      }
      currentDir.children[newDirName] = newDir
      markAncestorsAsDirty(currentDir)

      const filePath = dirPath + '/filename'
      const file = {
        path: filePath,
        content: values([Buffer.from('file with path ' + filePath)])
      }
      source.push(file)
      if (currentDir.depth === 0 || childCount + 1 === maxEntriesPerDir) {
        currentDir = newDir
      }
      importer.flush((err, hash) => {
        expect(err).to.not.exist()
        expect(Buffer.isBuffer(hash)).to.be.true()
        testAllYielded(tree)
        if (currentDir.depth < maxDepth) {
          pushAndFlush()
        } else {
          expect(count).to.be.eql(38)
          done()
        }
      })
    }

    function markDirAsYielded (node) {
      const dir = findDir(tree, node.path)
      if (node.path === dir.path) {
        expect(dir.yielded).to.be.false()
        dir.yielded = true
      }
    }

    function findDir (tree, path) {
      const pathElems = path.split('/').filter(notEmpty)
      const child = tree.children[pathElems.shift()]
      if (!child) {
        return tree
      }
      if (pathElems.length) {
        return findDir(child, pathElems.join('/'))
      } else {
        return child
      }
    }

    function testAllYielded (tree) {
      if (tree.depth) {
        expect(tree.yielded).to.be.true()
      }
      const childrenNames = Object.keys(tree.children)
      childrenNames.forEach((childName) => {
        const child = tree.children[childName]
        testAllYielded(child)
      })
    }

    function markAncestorsAsDirty (dir) {
      dir.yielded = false
      while (dir) {
        dir = dir.parent
        if (dir) {
          dir.yielded = false
        }
      }
    }
  })
})

function notEmpty (str) {
  return Boolean(str)
}
